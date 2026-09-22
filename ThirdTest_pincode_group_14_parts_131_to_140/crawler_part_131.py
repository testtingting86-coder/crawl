"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 131 / 400
================================================================================
- Group: ThirdTest_pincode_group_14_parts_131_to_140
- Assigned PIN Codes: 48 (Range: 415527 to 415713)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_131.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_131.csv & .json
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

PART_ID = "part_131"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-131] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "415527",
  "415528",
  "415530",
  "415536",
  "415537",
  "415538",
  "415539",
  "415540",
  "415601",
  "415602",
  "415603",
  "415604",
  "415605",
  "415606",
  "415607",
  "415608",
  "415609",
  "415610",
  "415611",
  "415612",
  "415613",
  "415614",
  "415615",
  "415616",
  "415617",
  "415619",
  "415620",
  "415621",
  "415626",
  "415628",
  "415629",
  "415634",
  "415637",
  "415639",
  "415640",
  "415641",
  "415643",
  "415701",
  "415702",
  "415703",
  "415705",
  "415706",
  "415708",
  "415709",
  "415710",
  "415711",
  "415712",
  "415713"
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
  "415527": {
    "pincode": "415527",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Ambheri B.O",
      "Bhosare B.O",
      "Kokrale B.O",
      "Kumathe Nagache B.O",
      "Loni B.O",
      "Varud B.O",
      "Kuroli Sidheshwar S.O"
    ]
  },
  "415528": {
    "pincode": "415528",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Kalaj B.O",
      "Nandal B.O",
      "Nimbhore B.O",
      "Rawadi BK B.O",
      "Surawadi B.O",
      "Taradgaon S.O"
    ]
  },
  "415530": {
    "pincode": "415530",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Jamb (Kikali) B.O",
      "Kikali B.O",
      "Kisanveernagar S.O"
    ]
  },
  "415536": {
    "pincode": "415536",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Chandak B.O",
      "Gulumb B.O",
      "Kenjal S.O"
    ]
  },
  "415537": {
    "pincode": "415537",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Adarki Khurd B.O",
      "Bibi B.O",
      "Ghadgewadi B.O",
      "Kapshi B.O",
      "Adarki BK S.O"
    ]
  },
  "415538": {
    "pincode": "415538",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Chitali B.O",
      "Chorade B.O",
      "Holichagaon B.O",
      "Nimsod B.O",
      "Shenawadi B.O",
      "Mhasurne S.O"
    ]
  },
  "415539": {
    "pincode": "415539",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Agashivnagar B.O",
      "Atake B.O",
      "Belawade BK B.O",
      "Kalawade B.O",
      "Kasarshirambe B.O",
      "Nandalapur B.O",
      "Narayanwadi B.O",
      "Vathar KH B.O",
      "Malkapur Karad S.O"
    ]
  },
  "415540": {
    "pincode": "415540",
    "circle": "Maharashtra circle",
    "region": "Pune Region",
    "division": "Satara Division",
    "offices": [
      "Gondavale KHD B.O",
      "Naravane B.O",
      "Palshi (Man) B.O",
      "Pimpri (Man) B.O",
      "Virali B.O",
      "Gondavale BK S.O"
    ]
  },
  "415601": {
    "pincode": "415601",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Pophali S.O"
    ]
  },
  "415602": {
    "pincode": "415602",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Shirgaon (CPN) S.O",
      "Kumbharli B.O",
      "Mundhe B.O",
      "Talsar B.O"
    ]
  },
  "415603": {
    "pincode": "415603",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Alore S.O",
      "Khadpoli B.O",
      "Kolke Wadi B.O",
      "Pedhambe B.O"
    ]
  },
  "415604": {
    "pincode": "415604",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Kherdi S.O",
      "Adre B.O",
      "Akale B.O",
      "Anari B.O",
      "Chinchghari B.O",
      "Dalvatane B.O",
      "Dhamnavane B.O",
      "Kadwad B.O",
      "Kalkavane B.O",
      "Kanhe B.O",
      "Moravane B.O",
      "Nandivase B.O",
      "Ovali B.O",
      "Pimpli Khurd B.O",
      "Tiware B.O",
      "Vehele B.O"
    ]
  },
  "415605": {
    "pincode": "415605",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Chiplun H.O",
      "Kelne B.O",
      "Dhamandevi B.O",
      "Pedhe B.O",
      "Ambdas B.O",
      "Bhile B.O",
      "Kadavali B.O",
      "Kalambaste B.O",
      "Walope B.O",
      "Bhelsai B.O",
      "Chirni B.O",
      "Musad B.O",
      "Teru B.O",
      "Kamathe B.O",
      "Pali(CPN) B.O",
      "Kapsal B.O",
      "Parshuram B.O",
      "Kalvande B.O",
      "Kaluste B.O",
      "Markandi S.O"
    ]
  },
  "415606": {
    "pincode": "415606",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Sawarda S.O",
      "Agave B.O",
      "Asurda B.O",
      "Dahivali Budruk B.O",
      "Dervan B.O",
      "Durgawadi B.O",
      "Furus B.O",
      "Kondmala B.O",
      "Kosbi B.O",
      "Mandki B.O",
      "Mudhe Tarf Sawarda B.O"
    ]
  },
  "415607": {
    "pincode": "415607",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Nandgaon (CPN) S.O",
      "Kumbharkhani B.O",
      "Kutra B.O",
      "Pachambe B.O",
      "Rajivili B.O",
      "Talavade B.O",
      "Yegaon B.O"
    ]
  },
  "415608": {
    "pincode": "415608",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Makhajan S.O",
      "Ambav B.O",
      "Aravali B.O",
      "Burambad B.O",
      "Dhamapur B.O",
      "Kalambushi B.O",
      "Karjuve B.O",
      "Kase B.O",
      "Kokare B.O",
      "Kondivare B.O",
      "Narduve B.O",
      "Nayashi B.O",
      "Murdav B.O"
    ]
  },
  "415609": {
    "pincode": "415609",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Kadwai S.O",
      "Chikhali B.O",
      "Masrang B.O",
      "Tural B.O"
    ]
  },
  "415610": {
    "pincode": "415610",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Kasba S.O (Ratnagiri)",
      "Antravali B.O",
      "Kalambaste B.O",
      "Karbhatale B.O",
      "Nayari B.O",
      "Phansavane B.O",
      "Shringarpur B.O",
      "Tambedi B.O",
      "Tiware Ghera Prachitgad B.O",
      "Umare B.O"
    ]
  },
  "415611": {
    "pincode": "415611",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Sangmeshwar S.O",
      "Deole Ghera Prachitgad B.O",
      "Dhamani B.O",
      "Dingni B.O",
      "Fungus B.O",
      "Golavali B.O",
      "Kurdhunda B.O",
      "Lowale B.O",
      "Mabhale B.O",
      "Muchari B.O",
      "Pirandavane B.O",
      "Rajwadi B.O",
      "Shembavane B.O",
      "Sonavade B.O",
      "Terye B.O",
      "Washi Tarf Sangmeshwar B.O"
    ]
  },
  "415612": {
    "pincode": "415612",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Ratnagiri H.O",
      "Kelye B.O",
      "Phansavale B.O",
      "Chinchkhari B.O",
      "Someshwar B.O",
      "Phansop B.O",
      "Bhatye B.O",
      "Dande Adom B.O",
      "Pomendi Khurd B.O",
      "Pomendi Budruk B.O",
      "Tembye B.O",
      "Tonade B.O",
      "Kolambe B.O",
      "Karla B.O",
      "Majgaon",
      "Shivaji Nagar (RTG) S.O",
      "Tilak Memorial (RTG) S.O",
      "Ratnagiri Collectorate S.O",
      "Mirya B.O"
    ]
  },
  "415613": {
    "pincode": "415613",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Saitavada S.O",
      "Jambhari B.O",
      "Kudali B.O",
      "Padve B.O",
      "Tavsal B.O",
      "Watad B.O"
    ]
  },
  "415614": {
    "pincode": "415614",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Jaigad S.O"
    ]
  },
  "415615": {
    "pincode": "415615",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Malgund S.O",
      "Ganpatipule B.O",
      "Nivendi B.O"
    ]
  },
  "415616": {
    "pincode": "415616",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Pawas S.O",
      "Agave B.O",
      "Chandor B.O",
      "Ganeshgule B.O",
      "Golap B.O",
      "Mavlange B.O",
      "Mervi B.O",
      "Nakhare B.O",
      "Nirul B.O",
      "Purnagad B.O"
    ]
  },
  "415617": {
    "pincode": "415617",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Kotawada S.O",
      "Pirandavane B.O",
      "Vetoshi B.O"
    ]
  },
  "415619": {
    "pincode": "415619",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Hatkhamba S.O",
      "Charveli B.O",
      "Kapad Gaon B.O",
      "Panval B.O",
      "Velvand B.O",
      "Zare Wadi B.O"
    ]
  },
  "415620": {
    "pincode": "415620",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Khalgaon S.O",
      "Chafe B.O",
      "Chave B.O",
      "Deud B.O",
      "Dhamanse B.O",
      "Kalzondi B.O",
      "Kespuri B.O",
      "Kolisare B.O",
      "Kondye B.O",
      "Manjare B.O",
      "Nevare B.O",
      "Ori B.O",
      "Pochari B.O",
      "Rai B.O",
      "Ranpat B.O",
      "Tarval B.O",
      "Varawade B.O"
    ]
  },
  "415621": {
    "pincode": "415621",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Bharane S.O",
      "Kalamani Budruk B.O",
      "Nilavane B.O",
      "Sukivili B.O",
      "Veral B.O"
    ]
  },
  "415626": {
    "pincode": "415626",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Gavkhadi S.O",
      "Dorle B.O",
      "Gavade Ambere B.O",
      "Shivar Ambere B.O"
    ]
  },
  "415628": {
    "pincode": "415628",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Shiral S.O",
      "Bhom B.O",
      "Biwali B.O",
      "Donavali B.O",
      "Gondhale B.O",
      "Karjikar Mohalla B.O",
      "Kapre B.O",
      "Karambavane B.O",
      "Kharvate B.O",
      "Khopad B.O",
      "Maldoli B.O",
      "Omali B.O",
      "Posare B.O",
      "Tamhanmala B.O"
    ]
  },
  "415629": {
    "pincode": "415629",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Shirgaon (RTG) S.O",
      "Basani B.O",
      "Kalbadevi B.O",
      "Kasarveli B.O"
    ]
  },
  "415634": {
    "pincode": "415634",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "RGPPL Anjanvel S.O",
      "Anjanvel B.O"
    ]
  },
  "415637": {
    "pincode": "415637",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Ambed Budruk S.O",
      "Kolambe(S) B.O",
      "Parchuri B.O",
      "Talekante B.O",
      "Ukshi B.O",
      "Vandri B.O"
    ]
  },
  "415639": {
    "pincode": "415639",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "MIDC (RTG) S.O",
      "Bhoke B.O",
      "Ghodavali B.O",
      "Karbude B.O",
      "Khedashi B.O",
      "Mirjole B.O",
      "Nachane B.O",
      "Niwali B.O",
      "Pangari B.O",
      "Kuvarbav S.O"
    ]
  },
  "415640": {
    "pincode": "415640",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Khavati S.O",
      "Chinchavali B.O",
      "Kashedi B.O",
      "Natunagar B.O",
      "Tulshi Khurd B.O",
      "Udhale Budruk B.O"
    ]
  },
  "415641": {
    "pincode": "415641",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Niwali (CPN) S.O",
      "Ambitgaon B.O",
      "Dhakmoli B.O",
      "Kalambat B.O",
      "Murtavade B.O",
      "Palvan B.O",
      "Pate Pillavali B.O",
      "Pilavali B.O",
      "Shirambe B.O",
      "Tondali B.O",
      "Turambav B.O",
      "Vahal B.O",
      "Veer B.O"
    ]
  },
  "415643": {
    "pincode": "415643",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Watul S.O",
      "Argaon B.O",
      "Kondge B.O",
      "Kurang B.O",
      "Panhale B.O",
      "Ringane B.O",
      "Shiravali B.O",
      "Vhel B.O",
      "Vilavade B.O",
      "Waghangaon B.O",
      "Waked B.O"
    ]
  },
  "415701": {
    "pincode": "415701",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Rampur S.O (Ratnagiri)",
      "Bamnoli B.O",
      "Borgaon B.O",
      "Chiveli B.O",
      "Ghude B.O",
      "Katroli B.O",
      "Malghar B.O",
      "Miravane B.O",
      "Nirvhal B.O",
      "Vadad B.O",
      "Waghivare B.O"
    ]
  },
  "415702": {
    "pincode": "415702",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Margatamhane S.O",
      "Gimvi B.O",
      "Kalamundi B.O",
      "Kaundhar Tamhane B.O",
      "Kutgiri B.O",
      "Tanali B.O",
      "Ubhale B.O",
      "Umroli B.O",
      "Zombadi B.O"
    ]
  },
  "415703": {
    "pincode": "415703",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Guhagar S.O",
      "Are B.O",
      "Asgoli B.O",
      "Dhopave B.O",
      "Jamsud B.O",
      "Kotluk B.O",
      "Palshet B.O",
      "Pimpar B.O",
      "Varveli B.O",
      "Veldur B.O"
    ]
  },
  "415705": {
    "pincode": "415705",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Adur S.O (Ratnagiri)"
    ]
  },
  "415706": {
    "pincode": "415706",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Dabhol S.O",
      "Oni navase B.O",
      "Umbarghar B.O"
    ]
  },
  "415708": {
    "pincode": "415708",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Lavel S.O",
      "Asgani B.O",
      "Ayani B.O",
      "Satvingaon B.O"
    ]
  },
  "415709": {
    "pincode": "415709",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Khed (RTG) S.O",
      "Alsure B.O",
      "Ambaye B.O",
      "Ashti B.O",
      "Boraj B.O",
      "Chinchghar B.O",
      "Dhamani B.O",
      "Hedali B.O",
      "Jamage B.O",
      "Kondivali B.O",
      "Koregaon B.O",
      "Mandva B.O",
      "Mani B.O",
      "Morvande B.O",
      "Murde B.O",
      "Nandgaon (Khed) B.O",
      "Sakhroli B.O",
      "Sanglat B.O",
      "Saveni B.O",
      "Shingri B.O",
      "Shiv Budruk B.O",
      "Shivtar B.O",
      "Susheri B.O",
      "Tala Mandva B.O",
      "Tisangi B.O",
      "Tise B.O",
      "Khed City S.O"
    ]
  },
  "415710": {
    "pincode": "415710",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Furus S.O",
      "Dayal B.O",
      "Poynar B.O"
    ]
  },
  "415711": {
    "pincode": "415711",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Vakavali S.O",
      "Asond B.O",
      "Bhopan B.O",
      "Dabhil B.O",
      "Degaon B.O",
      "Kolbandre B.O",
      "Pangari Tarf Haveli B.O",
      "Panhale Kazi B.O",
      "Phansu B.O",
      "Tetavali B.O",
      "Wavghar B.O"
    ]
  },
  "415712": {
    "pincode": "415712",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Dapoli S.O",
      "Agarwaingani B.O",
      "Chickalgaon B.O",
      "Dehen B.O",
      "Kadivali B.O",
      "Kalambat B.O",
      "Kangwai B.O",
      "Karanjani B.O",
      "Kherdi B.O",
      "Kolthare B.O",
      "Kudavale B.O",
      "Ladghar B.O",
      "Matwan B.O",
      "Nante B.O",
      "Pachavali B.O",
      "Palavani B.O",
      "Panchnadi B.O",
      "Pisai B.O",
      "Sakhloli B.O",
      "Sarang B.O",
      "Shirsoli B.O",
      "Tadil B.O",
      "Talsure B.O",
      "Tangar B.O",
      "Umbarle B.O",
      "Varnoshi Tarf Panchanadi B.O",
      "Velvi B.O",
      "Jalgaon B.O"
    ]
  },
  "415713": {
    "pincode": "415713",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Harnai S.O",
      "Asud B.O",
      "Karde B.O",
      "Murud B.O",
      "Paj B.O"
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
