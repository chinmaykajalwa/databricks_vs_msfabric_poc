"""
Static reference catalogues.
============================
Hand-curated so the data reads as plausible to someone who knows the
medical-surgical supply business, while every company, plant, DC and customer
name is FICTIONAL. Cities and coordinates are real places used only for
geography; no site here corresponds to a real company's facility.

Profiles take the first N entries of each catalogue, so `lite` is a strict
subset of `baseline` -- the same plants and DCs, just fewer of them.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Plants. Capabilities decide what each plant can make or sterilise.
# ---------------------------------------------------------------------------
PLANTS = [
    {"plant_id": "P01", "name": "Kenosha Molding & Kitting", "city": "Kenosha", "state": "WI",
     "lat": 42.5847, "lon": -87.8212, "size_sqft": 640_000,
     "capabilities": ("molding", "kit_assembly", "eto_sterilization")},
    {"plant_id": "P02", "name": "Lafayette Textiles & Molding", "city": "Lafayette", "state": "IN",
     "lat": 40.4167, "lon": -86.8753, "size_sqft": 520_000,
     "capabilities": ("textiles", "molding", "eto_sterilization")},
    {"plant_id": "P03", "name": "Columbus Sterile Kitting", "city": "Columbus", "state": "OH",
     "lat": 39.9612, "lon": -82.9988, "size_sqft": 450_000,
     "capabilities": ("kit_assembly", "eto_sterilization")},
    {"plant_id": "P04", "name": "El Paso Molding & Textiles", "city": "El Paso", "state": "TX",
     "lat": 31.7619, "lon": -106.4850, "size_sqft": 700_000,
     "capabilities": ("molding", "textiles")},
    {"plant_id": "P05", "name": "Greenville Wound Care", "city": "Greenville", "state": "SC",
     "lat": 34.8526, "lon": -82.3940, "size_sqft": 310_000,
     "capabilities": ("wound_care", "kit_assembly")},
]

# Share of molding machines per plant (normalised over the plants in profile).
MOLDING_WEIGHT = {"P01": 0.45, "P02": 0.30, "P04": 0.25}

# ---------------------------------------------------------------------------
# Distribution centres. `towns` drive customer geography; all within
# ~170 km so every route fits a 14-hour on-duty window.
# ---------------------------------------------------------------------------
DCS = [
    {"dc_id": "DC01", "metro": "Chicago", "city": "Joliet", "state": "IL", "lat": 41.5250,
     "lon": -88.0817, "region": "Midwest", "size_sqft": 1_300_000, "automation": "high",
     "outage_default": False,
     "towns": [("Chicago", "IL", 41.8781, -87.6298), ("Naperville", "IL", 41.7508, -88.1535),
               ("Rockford", "IL", 42.2711, -89.0940), ("Aurora", "IL", 41.7606, -88.3201),
               ("Evanston", "IL", 42.0451, -87.6877), ("Elgin", "IL", 42.0354, -88.2826)]},
    {"dc_id": "DC02", "metro": "Indianapolis", "city": "Plainfield", "state": "IN", "lat": 39.7042,
     "lon": -86.3994, "region": "Midwest", "size_sqft": 900_000, "automation": "high",
     "outage_default": True,
     "towns": [("Indianapolis", "IN", 39.7684, -86.1581), ("Bloomington", "IN", 39.1653, -86.5264),
               ("Lafayette", "IN", 40.4167, -86.8753), ("Muncie", "IN", 40.1934, -85.3864),
               ("Columbus", "IN", 39.2014, -85.9214), ("Richmond", "IN", 39.8289, -84.8902)]},
    {"dc_id": "DC03", "metro": "Atlanta", "city": "McDonough", "state": "GA", "lat": 33.4473,
     "lon": -84.1469, "region": "Southeast", "size_sqft": 1_000_000, "automation": "medium",
     "outage_default": False,
     "towns": [("Atlanta", "GA", 33.7490, -84.3880), ("Macon", "GA", 32.8407, -83.6324),
               ("Athens", "GA", 33.9519, -83.3576), ("Rome", "GA", 34.2570, -85.1647),
               ("Milledgeville", "GA", 33.0801, -83.2321), ("Columbus", "GA", 32.4610, -84.9877)]},
    {"dc_id": "DC04", "metro": "Dallas", "city": "Lancaster", "state": "TX", "lat": 32.5921,
     "lon": -96.7561, "region": "South", "size_sqft": 1_100_000, "automation": "high",
     "outage_default": False,
     "towns": [("Dallas", "TX", 32.7767, -96.7970), ("Fort Worth", "TX", 32.7555, -97.3308),
               ("Waco", "TX", 31.5493, -97.1467), ("Tyler", "TX", 32.3513, -95.3011),
               ("Denton", "TX", 33.2148, -97.1331), ("Sherman", "TX", 33.6357, -96.6089)]},
    {"dc_id": "DC05", "metro": "Sacramento", "city": "West Sacramento", "state": "CA", "lat": 38.5805,
     "lon": -121.5302, "region": "West", "size_sqft": 950_000, "automation": "high",
     "outage_default": False,
     "towns": [("Sacramento", "CA", 38.5816, -121.4944), ("San Francisco", "CA", 37.7749, -122.4194),
               ("Oakland", "CA", 37.8044, -122.2712), ("Modesto", "CA", 37.6391, -120.9969),
               ("Auburn", "CA", 38.8966, -121.0769), ("San Jose", "CA", 37.3382, -121.8863)]},
    {"dc_id": "DC06", "metro": "Inland Empire", "city": "Riverside", "state": "CA", "lat": 33.9533,
     "lon": -117.3962, "region": "West", "size_sqft": 1_050_000, "automation": "high",
     "outage_default": False,
     "towns": [("Los Angeles", "CA", 34.0522, -118.2437), ("San Diego", "CA", 32.7157, -117.1611),
               ("Riverside", "CA", 33.9533, -117.3962), ("Anaheim", "CA", 33.8366, -117.9143),
               ("Temecula", "CA", 33.4936, -117.1484), ("Palm Springs", "CA", 33.8303, -116.5453)]},
    {"dc_id": "DC07", "metro": "Lehigh Valley", "city": "Allentown", "state": "PA", "lat": 40.6084,
     "lon": -75.4902, "region": "Northeast", "size_sqft": 1_000_000, "automation": "high",
     "outage_default": False,
     "towns": [("Philadelphia", "PA", 39.9526, -75.1652), ("Newark", "NJ", 40.7357, -74.1724),
               ("Allentown", "PA", 40.6084, -75.4902), ("Scranton", "PA", 41.4090, -75.6624),
               ("Trenton", "NJ", 40.2206, -74.7597), ("Wilmington", "DE", 39.7391, -75.5398)]},
    {"dc_id": "DC08", "metro": "Charlotte", "city": "Concord", "state": "NC", "lat": 35.4088,
     "lon": -80.5795, "region": "Southeast", "size_sqft": 800_000, "automation": "medium",
     "outage_default": False,
     "towns": [("Charlotte", "NC", 35.2271, -80.8431), ("Winston-Salem", "NC", 36.0999, -80.2442),
               ("Greensboro", "NC", 36.0726, -79.7920), ("Rock Hill", "SC", 34.9249, -81.0251),
               ("Hickory", "NC", 35.7332, -81.3412), ("Salisbury", "NC", 35.6710, -80.4742)]},
    {"dc_id": "DC09", "metro": "Denver", "city": "Aurora", "state": "CO", "lat": 39.7294,
     "lon": -104.8319, "region": "Mountain", "size_sqft": 700_000, "automation": "medium",
     "outage_default": False,
     "towns": [("Denver", "CO", 39.7392, -104.9903), ("Colorado Springs", "CO", 38.8339, -104.8214),
               ("Boulder", "CO", 40.0150, -105.2705), ("Fort Collins", "CO", 40.5853, -105.0844),
               ("Pueblo", "CO", 38.2544, -104.6091), ("Cheyenne", "WY", 41.1400, -104.8202)]},
    {"dc_id": "DC10", "metro": "Phoenix", "city": "Tolleson", "state": "AZ", "lat": 33.4501,
     "lon": -112.2593, "region": "Southwest", "size_sqft": 750_000, "automation": "medium",
     "outage_default": False,
     "towns": [("Phoenix", "AZ", 33.4484, -112.0740), ("Casa Grande", "AZ", 32.8795, -111.7574),
               ("Mesa", "AZ", 33.4152, -111.8315), ("Prescott", "AZ", 34.5400, -112.4685),
               ("Buckeye", "AZ", 33.3703, -112.5838), ("Scottsdale", "AZ", 33.4942, -111.9261)]},
]

# ---------------------------------------------------------------------------
# Resins and their validated molding windows (setpoint, +/- tolerance).
# Used now for molds, and in step 3 by the molding physics.
# ---------------------------------------------------------------------------
RESINS = {
    "PP":   {"component_id": "RES-PP-H",  "melt_c": (230, 8), "mold_c": (40, 5), "density": 0.90},
    "HDPE": {"component_id": "RES-HDPE",  "melt_c": (220, 8), "mold_c": (30, 5), "density": 0.95},
    "LDPE": {"component_id": "RES-LDPE",  "melt_c": (200, 8), "mold_c": (25, 5), "density": 0.92},
    "PS":   {"component_id": "RES-GPPS",  "melt_c": (220, 8), "mold_c": (45, 5), "density": 1.05},
    "PETG": {"component_id": "RES-PETG",  "melt_c": (255, 6), "mold_c": (20, 4), "density": 1.27},
}

# ---------------------------------------------------------------------------
# Purchased components (raw materials and packaging). Category -> supplier.
# ---------------------------------------------------------------------------
COMPONENTS = [
    # id, description, category, uom, unit_cost_usd
    ("RES-PP-H", "Polypropylene homopolymer, medical grade", "resin", "kg", 1.45),
    ("RES-PP-C", "Polypropylene random copolymer, clarified", "resin", "kg", 1.60),
    ("RES-HDPE", "High-density polyethylene, medical grade", "resin", "kg", 1.35),
    ("RES-LDPE", "Low-density polyethylene, medical grade", "resin", "kg", 1.40),
    ("RES-GPPS", "General-purpose polystyrene", "resin", "kg", 1.55),
    ("RES-PETG", "PETG copolyester", "resin", "kg", 2.90),
    ("NW-SMS-35", "SMS nonwoven 35 gsm", "nonwoven", "m2", 0.32),
    ("NW-SMS-45", "SMS nonwoven 45 gsm", "nonwoven", "m2", 0.41),
    ("NW-SMS-60", "SMS nonwoven 60 gsm, AAMI L3", "nonwoven", "m2", 0.58),
    ("NW-SPL-RF", "Spunlace reinforcement panel", "nonwoven", "ea", 0.22),
    ("NW-ABS-RF", "Absorbent fenestration reinforcement", "nonwoven", "ea", 0.35),
    ("TX-CUFF", "Knit cuff, polyester", "nonwoven", "pair", 0.09),
    ("TX-TIE", "Neck/waist tie tape", "nonwoven", "m", 0.03),
    ("TX-HOOK", "Hook-and-loop neck closure", "nonwoven", "ea", 0.04),
    ("PKG-TYV-S", "Tyvek lid, 8x10 in", "medical_film", "ea", 0.11),
    ("PKG-TYV-L", "Tyvek lid, 16x24 in", "medical_film", "ea", 0.34),
    ("PKG-POU-S", "Peel pouch, 6x10 in", "medical_film", "ea", 0.05),
    ("PKG-POU-L", "Peel pouch, 12x18 in", "medical_film", "ea", 0.12),
    ("PKG-CSR-36", "CSR wrap 36x36 in", "medical_film", "ea", 0.21),
    ("PKG-CSR-45", "CSR wrap 45x45 in", "medical_film", "ea", 0.29),
    ("PKG-TRAY-S", "Thermoformed PETG tray, small", "packaging", "ea", 0.38),
    ("PKG-TRAY-M", "Thermoformed PETG tray, medium", "packaging", "ea", 0.62),
    ("PKG-TRAY-L", "Thermoformed PETG tray, large", "packaging", "ea", 0.95),
    ("PKG-BAG", "Poly bag, 4 mil", "packaging", "ea", 0.02),
    ("PKG-CTN-S", "Corrugated shipper, small", "packaging", "ea", 0.48),
    ("PKG-CTN-M", "Corrugated shipper, medium", "packaging", "ea", 0.72),
    ("PKG-CTN-L", "Corrugated shipper, large", "packaging", "ea", 1.05),
    ("PKG-LBL-P", "Product label with UDI", "packaging", "ea", 0.01),
    ("PKG-LBL-C", "Case label with UDI", "packaging", "ea", 0.02),
    ("PKG-IFU", "Instructions for use leaflet", "packaging", "ea", 0.02),
    ("WC-PUF", "Polyurethane foam sheet", "wound_materials", "m2", 4.80),
    ("WC-SIL", "Silicone adhesive laminate", "wound_materials", "m2", 6.20),
    ("WC-HYD", "Hydrogel bulk", "wound_materials", "kg", 9.50),
    ("WC-ALG", "Calcium alginate fibre pad", "wound_materials", "m2", 7.40),
    ("WC-GAU", "Cotton gauze pad, 12-ply", "wound_materials", "ea", 0.03),
    ("WC-FILM", "Polyurethane film, breathable", "wound_materials", "m2", 2.10),
    ("WC-AG", "Ionic silver antimicrobial layer", "wound_materials", "m2", 14.00),
    ("WC-COL", "Collagen matrix sheet", "wound_materials", "m2", 22.00),
    ("STL-ETO", "Ethylene oxide sterilant, 100%", "sterilant_gas", "kg", 6.50),
]

# Supplier categories: (category, preferred countries, name nouns)
SUPPLIER_CATEGORIES = [
    ("resin", ("US", "US", "DE", "KR"), ("Polymers", "Resins", "Polyolefins")),
    ("nonwoven", ("US", "MX", "CN", "IN"), ("Nonwovens", "Textiles", "Fabrics")),
    ("medical_film", ("US", "US", "DE"), ("Packaging Films", "Medical Packaging", "Barrier Films")),
    ("packaging", ("US", "MX", "US"), ("Packaging", "Containers", "Corrugated")),
    ("wound_materials", ("US", "DE", "IN"), ("BioMaterials", "Advanced Materials", "Dressings")),
    ("sterilant_gas", ("US", "US"), ("Specialty Gases", "Industrial Gases")),
    ("glove_oem", ("MY", "TH", "VN", "MY"), ("Gloves", "Rubber Products", "Latex Industries")),
    ("finished_goods_oem", ("CN", "IN", "MX", "VN", "US"), ("Medical Products", "Healthcare Manufacturing", "Health Products")),
]
SUPPLIER_ADJ = ["Summit", "Harbor", "Golden Leaf", "Northstar", "Bluewater", "Ironwood", "Crescent",
                "Pioneer", "Evergreen", "Silverline", "Keystone", "Meridian", "Riverbend", "Granite",
                "Lakeside", "Sunrise", "Westfield", "Orchid", "Atlas", "Cedar Point"]
COUNTRY_SUFFIX = {"US": ("Inc.", "LLC", "Corp."), "MX": ("S.A. de C.V.",), "DE": ("GmbH",),
                  "KR": ("Co., Ltd.",), "CN": ("Co., Ltd.",), "IN": ("Pvt. Ltd.",),
                  "MY": ("Sdn. Bhd.",), "TH": ("Co., Ltd.",), "VN": ("JSC",)}
COUNTRY_LEAD_DAYS = {"US": (7, 21), "MX": (14, 28), "DE": (30, 50), "KR": (40, 60),
                     "CN": (45, 75), "IN": (45, 80), "MY": (45, 70), "TH": (45, 70), "VN": (50, 80)}

# ---------------------------------------------------------------------------
# SKU templates per family.
#   make_capability : plant capability that makes it in-house (None = always bought)
#   supplier_category for bought items
# ---------------------------------------------------------------------------
FAMILIES = {
    "molded_disposables": {"share": 0.20, "make_capability": "molding", "supplier_category": "finished_goods_oem",
                           "each_per_case": (50, 100, 200, 500), "cost": (0.12, 4.50)},
    "textiles":           {"share": 0.15, "make_capability": "textiles", "supplier_category": "finished_goods_oem",
                           "each_per_case": (20, 30, 50, 100), "cost": (1.20, 12.00)},
    "procedure_kits":     {"share": 0.15, "make_capability": "kit_assembly", "supplier_category": "finished_goods_oem",
                           "each_per_case": (2, 4, 6, 10), "cost": (18.00, 185.00)},
    "wound_care":         {"share": 0.15, "make_capability": "wound_care", "supplier_category": "finished_goods_oem",
                           "each_per_case": (50, 100, 200), "cost": (0.40, 16.00)},
    "exam_gloves":        {"share": 0.15, "make_capability": None, "supplier_category": "glove_oem",
                           "each_per_case": (1000, 2000), "cost": (0.03, 1.20)},
    "general_supplies":   {"share": 0.20, "make_capability": None, "supplier_category": "finished_goods_oem",
                           "each_per_case": (12, 24, 100, 300), "cost": (0.05, 22.00)},
}

# (base name, variants, attrs). attrs keys:
#   sterile: True/False/"both"; cls: FDA device class; resin; part_g (min,max);
#   temp: (min_c, max_c) storage constraint -> temperature-sensitive item
SKU_TEMPLATES = {
    "molded_disposables": [
        ("Specimen Container", ["90 mL", "120 mL", "4 oz Screw Cap"], {"cost": (0.18, 0.45), "sterile": "both", "cls": "I", "resin": "PP", "part_g": (12, 20)}),
        ("Graduated Medicine Cup", ["30 mL", "60 mL"], {"cost": (0.02, 0.06), "sterile": False, "cls": "I", "resin": "PP", "part_g": (2, 4)}),
        ("Wash Basin", ["5 qt", "6 qt", "7 qt"], {"cost": (0.9, 2.2), "sterile": "both", "cls": "I", "resin": "PP", "part_g": (80, 150)}),
        ("Emesis Basin", ["500 mL", "700 mL"], {"cost": (0.25, 0.6), "sterile": "both", "cls": "I", "resin": "PP", "part_g": (25, 40)}),
        ("Suction Canister", ["1200 mL", "1500 mL", "3000 mL"], {"cost": (1.1, 3.2), "sterile": False, "cls": "II", "resin": "PP", "part_g": (90, 200)}),
        ("Sharps Container", ["1 qt", "5 qt", "2 gal"], {"cost": (1.5, 6.5), "sterile": False, "cls": "II", "resin": "HDPE", "part_g": (60, 260)}),
        ("Luer Lock Syringe", ["3 mL", "5 mL", "10 mL", "20 mL"], {"cost": (0.07, 0.22), "sterile": True, "cls": "II", "resin": "PP", "part_g": (3, 10)}),
        ("Irrigation Bulb Syringe", ["60 mL"], {"cost": (0.45, 0.95), "sterile": True, "cls": "I", "resin": "LDPE", "part_g": (18, 26)}),
        ("Denture Cup", ["8 oz"], {"cost": (0.2, 0.4), "sterile": False, "cls": "I", "resin": "PP", "part_g": (14, 20)}),
        ("Graduated Pitcher", ["32 oz", "1000 mL"], {"cost": (0.6, 1.2), "sterile": False, "cls": "I", "resin": "PP", "part_g": (55, 80)}),
        ("Bedpan", ["Standard", "Fracture"], {"cost": (1.4, 3.0), "sterile": False, "cls": "I", "resin": "HDPE", "part_g": (120, 180)}),
        ("Male Urinal", ["32 oz"], {"cost": (0.7, 1.4), "sterile": False, "cls": "I", "resin": "HDPE", "part_g": (60, 85)}),
        ("Petri Dish", ["60 mm", "100 mm"], {"cost": (0.08, 0.2), "sterile": True, "cls": "I", "resin": "PS", "part_g": (8, 18)}),
        ("Specimen Transport Tube", ["5 mL", "12 mL"], {"cost": (0.05, 0.15), "sterile": True, "cls": "I", "resin": "PS", "part_g": (2, 6)}),
    ],
    "textiles": [
        ("Surgical Gown, Non-Reinforced", ["L", "XL", "XXL"], {"cost": (2.2, 4.5), "sterile": True, "cls": "II", "fabric": "NW-SMS-45"}),
        ("Surgical Gown, Fabric-Reinforced", ["L", "XL", "XXL", "XXXL"], {"cost": (3.8, 7.5), "sterile": True, "cls": "II", "fabric": "NW-SMS-60"}),
        ("Isolation Gown, AAMI Level 2", ["Regular", "XL"], {"cost": (0.45, 1.1), "sterile": False, "cls": "II", "fabric": "NW-SMS-35"}),
        ("Patient Gown", ["Standard", "Bariatric"], {"cost": (0.9, 2.2), "sterile": False, "cls": "I", "fabric": "NW-SMS-35"}),
        ("Three-Quarter Drape Sheet", ["53x77 in"], {"cost": (1.6, 3.2), "sterile": True, "cls": "II", "fabric": "NW-SMS-45"}),
        ("Laparotomy Drape", ["Transverse", "Standard"], {"cost": (6.5, 12.0), "sterile": True, "cls": "II", "fabric": "NW-SMS-60"}),
        ("Split Sheet Drape", ["77x120 in"], {"cost": (7.0, 13.0), "sterile": True, "cls": "II", "fabric": "NW-SMS-60"}),
        ("Mayo Stand Cover", ["23x54 in"], {"cost": (0.8, 1.6), "sterile": True, "cls": "I", "fabric": "NW-SMS-45"}),
        ("Back Table Cover", ["44x90 in", "60x90 in"], {"cost": (1.5, 3.2), "sterile": True, "cls": "I", "fabric": "NW-SMS-45"}),
        ("Bouffant Cap", ["21 in", "24 in"], {"cost": (0.03, 0.08), "sterile": False, "cls": "I", "fabric": "NW-SMS-35"}),
        ("Shoe Cover", ["Universal", "XL"], {"cost": (0.06, 0.15), "sterile": False, "cls": "I", "fabric": "NW-SMS-35"}),
    ],
    "procedure_kits": [
        (f"{spec} Procedure Pack", ["Rev A", "Rev B", "Rev C", "Rev D", "Rev E", "Rev F"],
         {"sterile": True, "cls": "II", "tray": tray, "contents": {"PKG-TRAY-L": "surgical_L", "PKG-TRAY-M": "surgical_M", "PKG-TRAY-S": "surgical_S"}[tray], "cost": (40.0, 185.0)})
        for spec, tray in [
            ("Laparoscopy", "PKG-TRAY-L"), ("Laparotomy", "PKG-TRAY-L"), ("C-Section", "PKG-TRAY-L"),
            ("Vaginal Delivery", "PKG-TRAY-M"), ("Total Knee", "PKG-TRAY-L"), ("Total Hip", "PKG-TRAY-L"),
            ("Arthroscopy", "PKG-TRAY-M"), ("Cardiovascular", "PKG-TRAY-L"), ("Cataract", "PKG-TRAY-S"),
            ("ENT", "PKG-TRAY-M"), ("Spine", "PKG-TRAY-L"), ("Hand", "PKG-TRAY-M"),
            ("Cystoscopy", "PKG-TRAY-M"), ("Minor Surgery", "PKG-TRAY-S"), ("Angiography", "PKG-TRAY-M"),
        ]
    ] + [
        ("IV Start Kit", ["Standard", "with CHG"], {"cost": (1.4, 3.6), "sterile": True, "cls": "II", "contents": "dressing", "tray": "PKG-TRAY-S"}),
        ("Central Line Dressing Change Kit", ["Standard", "Pediatric"], {"cost": (4.5, 9.5), "sterile": True, "cls": "II", "contents": "dressing", "tray": "PKG-TRAY-S"}),
        ("Foley Catheter Insertion Tray", ["14 Fr", "16 Fr", "18 Fr"], {"cost": (6.0, 12.5), "sterile": True, "cls": "II", "contents": "bedside", "tray": "PKG-TRAY-M"}),
        ("Laceration Tray", ["Standard"], {"cost": (5.0, 10.0), "sterile": True, "cls": "I", "contents": "bedside", "tray": "PKG-TRAY-S"}),
        ("Lumbar Puncture Tray", ["Adult", "Pediatric"], {"cost": (14.0, 28.0), "sterile": True, "cls": "II", "contents": "bedside", "tray": "PKG-TRAY-M"}),
    ],
    "wound_care": [
        ("Silicone Bordered Foam Dressing", ["3x3 in", "4x4 in", "6x6 in", "Sacrum"], {"cost": (3.0, 12.0), "sterile": True, "cls": "I", "mat": "WC-PUF"}),
        ("Hydrogel Dressing", ["2x2 in", "4x4 in", "Amorphous 3 oz"], {"cost": (2.0, 6.0), "sterile": True, "cls": "I", "mat": "WC-HYD", "temp": (2, 30)}),
        ("Calcium Alginate Dressing", ["2x2 in", "4x4 in", "Rope 12 in"], {"cost": (2.0, 8.0), "sterile": True, "cls": "I", "mat": "WC-ALG"}),
        ("Bordered Gauze Dressing", ["4x4 in", "6x6 in"], {"cost": (0.3, 1.0), "sterile": True, "cls": "I", "mat": "WC-GAU"}),
        ("Transparent Film Dressing", ["2x3 in", "4x5 in"], {"cost": (0.3, 1.5), "sterile": True, "cls": "I", "mat": "WC-FILM"}),
        ("Collagen Wound Matrix", ["2x2 in", "4x4 in"], {"cost": (8.0, 20.0), "sterile": True, "cls": "II", "mat": "WC-COL", "temp": (2, 25)}),
        ("Silver Antimicrobial Foam", ["4x4 in", "6x6 in"], {"cost": (6.0, 16.0), "sterile": True, "cls": "II", "mat": "WC-AG"}),
    ],
    "exam_gloves": [
        ("Nitrile Exam Gloves, Powder-Free", ["XS", "S", "M", "L", "XL"], {"cost": (0.04, 0.09), "sterile": False, "cls": "I"}),
        ("Nitrile Exam Gloves, Chemo-Rated", ["S", "M", "L", "XL"], {"cost": (0.07, 0.14), "sterile": False, "cls": "I"}),
        ("Latex Exam Gloves", ["S", "M", "L"], {"cost": (0.03, 0.07), "sterile": False, "cls": "I"}),
        ("Vinyl Exam Gloves", ["S", "M", "L", "XL"], {"cost": (0.02, 0.05), "sterile": False, "cls": "I"}),
        ("Sterile Polyisoprene Surgical Gloves", ["6.0", "6.5", "7.0", "7.5", "8.0", "8.5"], {"epc": (200, 300), "cost": (1.2, 2.4), "sterile": True, "cls": "I"}),
        ("Sterile Latex Surgical Gloves", ["6.5", "7.0", "7.5", "8.0"], {"epc": (200, 300), "cost": (0.7, 1.4), "sterile": True, "cls": "I"}),
    ],
    "general_supplies": [
        ("Disposable Underpad", ["23x36 in", "30x30 in", "36x36 in"], {"epc": (100, 150), "cost": (0.15, 0.6), "sterile": False, "cls": "I"}),
        ("Adult Brief", ["S", "M", "L", "XL"], {"epc": (72, 96), "cost": (0.45, 1.2), "sterile": False, "cls": "I"}),
        ("Disinfectant Wipes", ["Canister 160 ct", "Soft Pack 80 ct"], {"epc": (12, 24), "cost": (4.0, 9.0), "sterile": False, "cls": "I"}),
        ("Hand Sanitizer Gel", ["8 oz", "1000 mL Refill"], {"epc": (12, 24), "cost": (1.5, 9.0), "sterile": False, "cls": "I"}),
        ("Alcohol Prep Pad", ["Medium", "Large"], {"epc": (2000, 4000), "cost": (0.01, 0.03), "sterile": True, "cls": "I"}),
        ("Procedure Face Mask, ASTM Level 3", ["Earloop", "Tie-On"], {"epc": (300, 500), "cost": (0.06, 0.18), "sterile": False, "cls": "II"}),
        ("N95 Respirator", ["Regular", "Small"], {"epc": (120, 240), "cost": (0.55, 1.4), "sterile": False, "cls": "II"}),
        ("Ultrasound Gel", ["250 mL", "5 L"], {"epc": (4, 12), "cost": (1.8, 22.0), "sterile": False, "cls": "I", "temp": (5, 30)}),
        ("Rapid Strep A Test Kit", ["25 Tests"], {"epc": (1, 4), "cost": (45.0, 95.0), "sterile": False, "cls": "II", "temp": (2, 30)}),
        ("Exam Table Paper", ["18 in Smooth", "21 in Crepe"], {"epc": (12,), "cost": (4.0, 9.0), "sterile": False, "cls": "I"}),
        ("Skin Barrier Cream", ["4 oz", "8 oz"], {"epc": (12, 24), "cost": (2.5, 6.5), "sterile": False, "cls": "I"}),
        ("Cotton-Tipped Applicator", ["3 in", "6 in"], {"epc": (1000, 2000), "cost": (0.01, 0.05), "sterile": "both", "cls": "I"}),
    ],
}

# ---------------------------------------------------------------------------
# Customer naming parts (all fictional combinations).
# ---------------------------------------------------------------------------
NATURE_WORDS = ["Cedar", "Willow", "Riverside", "Oak Hill", "Maple", "Prairie", "Lakeview", "Pine Ridge",
                "Silver Creek", "Meadowbrook", "Stonegate", "Highland", "Clearwater", "Birchwood",
                "Fox Run", "Summit", "Golden Valley", "Harbor Point", "Briar Glen", "Aspen"]
SAINTS = ["Agnes", "Brendan", "Clare", "Dominic", "Elias", "Francis", "Gabriel", "Helena", "Ignatius",
          "Jude", "Kateri", "Luke", "Monica", "Nicholas", "Raphael", "Theresa"]
SURNAMES = ["Abbott", "Bennett", "Castillo", "Delgado", "Ellison", "Fraser", "Gupta", "Hollis", "Iverson",
            "Jensen", "Kowalski", "Lindqvist", "Mendoza", "Nakamura", "Okafor", "Patel", "Quinlan",
            "Reyes", "Sorensen", "Thornton", "Underwood", "Vasquez", "Whitaker", "Yamada"]
FACILITY_SUFFIXES = ["North", "South", "East", "West", "Central", "Midtown", "Lakeside", "Parkway"]
SPECIALTIES = ["Family Medicine", "Orthopedic Associates", "Pediatrics", "Internal Medicine",
               "Dermatology", "Cardiology Group", "Women's Health", "Urgent Care"]

CUSTOMER_TYPES = {
    # share, IDN affiliation probability. Deliveries/week come from the
    # customer's route day-pattern, so they can never disagree with it.
    "hospital":         (0.12, 0.65),
    "ambulatory_surgery_center": (0.13, 0.40),
    "extended_care":    (0.30, 0.15),
    "physician_office": (0.35, 0.30),
    "home_health":      (0.10, 0.10),
}

# ---------------------------------------------------------------------------
# Equipment catalogues.
# ---------------------------------------------------------------------------
MOLDING_MAKES = ["Arburg Allrounder", "Engel e-mac", "Sumitomo SE-EV", "Milacron Elektron", "Husky HyPET"]
MOLDING_TONNAGES = (500, 350, 250, 150, 100)

DC_ASSET_TYPES = {
    # type: (share, zone, criticality, motor_kw range)
    "conveyor_segment":   (0.52, "transport", "standard", (0.75, 3.0)),
    "induction_station":  (0.06, "sortation", "important", (1.5, 4.0)),
    "crossbelt_sorter":   (0.04, "sortation", "critical", (30.0, 75.0)),
    "amr":                (0.24, "picking", "standard", (0.4, 1.2)),
    "shuttle_asrs":       (0.06, "storage", "important", (5.0, 11.0)),
    "palletizer":         (0.04, "outbound", "important", (7.5, 15.0)),
    "stretch_wrapper":    (0.04, "outbound", "standard", (2.2, 5.5)),
}

VEHICLE_TYPES = {
    "tractor_53ft": {"share": 0.60, "pallets": 26, "makes": ("Freightliner Cascadia", "Volvo VNL", "Kenworth T680")},
    "box_truck_26ft": {"share": 0.40, "pallets": 12, "makes": ("Isuzu FTR", "Freightliner M2 106", "International MV")},
}

# ---------------------------------------------------------------------------
# What a kit may contain, by content profile: (base product name, qty range).
# A large surgical pack carries gowns, drapes and basins; a small dressing or
# IV kit does not. Without this, a cystoscopy pack ends up containing
# ultrasound gel and exam-table paper -- the kind of detail a domain expert
# spots in seconds and that undermines everything else on the slide.
# ---------------------------------------------------------------------------
KIT_CONTENTS = {
    "surgical_L": [("Surgical Gown, Fabric-Reinforced", (1, 3)), ("Surgical Gown, Non-Reinforced", (1, 2)),
                   ("Laparotomy Drape", (1, 1)), ("Split Sheet Drape", (1, 1)), ("Three-Quarter Drape Sheet", (1, 2)),
                   ("Mayo Stand Cover", (1, 1)), ("Back Table Cover", (1, 1)), ("Wash Basin", (1, 2)),
                   ("Emesis Basin", (1, 1)), ("Graduated Medicine Cup", (1, 2)), ("Irrigation Bulb Syringe", (1, 1)),
                   ("Luer Lock Syringe", (1, 2)), ("Specimen Container", (1, 1)), ("Bordered Gauze Dressing", (2, 6)),
                   ("Sterile Polyisoprene Surgical Gloves", (1, 3)), ("Sterile Latex Surgical Gloves", (1, 2)),
                   ("Cotton-Tipped Applicator", (2, 6))],
    "surgical_M": [("Surgical Gown, Non-Reinforced", (1, 2)), ("Three-Quarter Drape Sheet", (1, 1)),
                   ("Mayo Stand Cover", (1, 1)), ("Wash Basin", (1, 1)), ("Graduated Medicine Cup", (1, 2)),
                   ("Luer Lock Syringe", (1, 2)), ("Specimen Container", (1, 1)), ("Bordered Gauze Dressing", (2, 4)),
                   ("Sterile Polyisoprene Surgical Gloves", (1, 2)), ("Cotton-Tipped Applicator", (2, 4))],
    "surgical_S": [("Three-Quarter Drape Sheet", (1, 1)), ("Graduated Medicine Cup", (1, 1)),
                   ("Luer Lock Syringe", (1, 1)), ("Bordered Gauze Dressing", (1, 2)),
                   ("Sterile Polyisoprene Surgical Gloves", (1, 2)), ("Cotton-Tipped Applicator", (2, 4))],
    "bedside":    [("Sterile Polyisoprene Surgical Gloves", (1, 1)), ("Sterile Latex Surgical Gloves", (1, 1)),
                   ("Disposable Underpad", (1, 1)), ("Specimen Container", (1, 1)), ("Luer Lock Syringe", (1, 2)),
                   ("Bordered Gauze Dressing", (2, 4)), ("Cotton-Tipped Applicator", (3, 5)),
                   ("Graduated Medicine Cup", (1, 1)), ("Alcohol Prep Pad", (2, 4))],
    "dressing":   [("Transparent Film Dressing", (1, 1)), ("Bordered Gauze Dressing", (1, 2)),
                   ("Alcohol Prep Pad", (2, 4)), ("Cotton-Tipped Applicator", (1, 3)),
                   ("Nitrile Exam Gloves, Powder-Free", (1, 2)), ("Procedure Face Mask, ASTM Level 3", (1, 2)),
                   ("Sterile Polyisoprene Surgical Gloves", (1, 1))],
}
KIT_LINES = {"surgical_L": (6, 10), "surgical_M": (4, 7), "surgical_S": (3, 5), "bedside": (4, 6), "dressing": (3, 5)}

# ---------------------------------------------------------------------------
# Quality deviation types. `key` is also the template key for the step-4
# deviation reports, so history, telemetry scenarios and documents share one
# vocabulary -- that is what lets an agent find precedent.
#   asset: which asset kind it attaches to; sev: (minor, major, critical) probs
# ---------------------------------------------------------------------------
DEVIATION_TYPES = [
    # key, category, asset, severity probs, root-cause category, title
    ("eto_rh_excursion", "sterilization", "sterilizer", (0.2, 0.6, 0.2), "equipment", "Chamber relative humidity outside validated range during exposure"),
    ("eto_rh_probe_drift", "sterilization", "sterilizer", (0.0, 0.7, 0.3), "calibration", "RH probe calibration drift found at verification"),
    ("eto_temp_excursion", "sterilization", "sterilizer", (0.3, 0.6, 0.1), "equipment", "Exposure temperature below validated minimum"),
    ("eto_low_gas_conc", "sterilization", "sterilizer", (0.1, 0.6, 0.3), "equipment", "EtO concentration below validated minimum"),
    ("eto_vacuum_leak", "sterilization", "sterilizer", (0.3, 0.6, 0.1), "equipment", "Leak-rate test failure before gas injection"),
    ("eto_bi_positive", "sterilization", "sterilizer", (0.0, 0.3, 0.7), "process", "Biological indicator positive after incubation"),
    ("mold_short_shot", "molding", "molding_machine", (0.6, 0.4, 0.0), "process", "Short shots detected on cavity pressure monitoring"),
    ("mold_flash", "molding", "molding_machine", (0.7, 0.3, 0.0), "equipment", "Flash at parting line exceeding specification"),
    ("mold_black_specks", "molding", "molding_machine", (0.4, 0.5, 0.1), "material", "Black specks / contamination in molded parts"),
    ("mold_dimensional_oos", "molding", "molding_machine", (0.5, 0.5, 0.0), "process", "Critical dimension out of specification"),
    ("pkg_seal_integrity", "packaging", "line", (0.2, 0.6, 0.2), "equipment", "Pouch seal strength below specification"),
    ("pkg_label_mixup", "packaging", "line", (0.0, 0.5, 0.5), "human", "Label / UDI mismatch with product in pouch"),
    ("txt_seam_defect", "textiles", "line", (0.7, 0.3, 0.0), "process", "Seam strength failure on gown sleeves"),
    ("kit_missing_component", "kit_assembly", "line", (0.4, 0.5, 0.1), "human", "Component missing from procedure pack"),
    ("sup_resin_coa", "supplier", "component_lot", (0.3, 0.6, 0.1), "material", "Resin certificate of analysis outside specification (melt flow index)"),
    ("dc_sorter_missort", "distribution", "dc_asset", (0.8, 0.2, 0.0), "equipment", "Cross-belt sorter mis-sort rate above threshold"),
    ("dc_temp_excursion", "distribution", "vehicle", (0.3, 0.6, 0.1), "equipment", "Reefer temperature excursion on temperature-sensitive load"),
]

CAPA_ACTIONS = {
    "equipment": ["Replace failed component and add to PM schedule", "Increase preventive maintenance frequency",
                  "Install redundant sensor with cross-check alarm"],
    "calibration": ["Increase calibration frequency from annual to quarterly", "Add redundant probe with drift alarm",
                    "Extend calibration program to all chambers"],
    "process": ["Revise process window and revalidate", "Update SOP and retrain operators", "Add in-process inspection step"],
    "material": ["Tighten incoming inspection on supplier lots", "Issue SCAR to supplier", "Qualify alternate supplier"],
    "human": ["Retrain line staff and add second-person verification", "Introduce vision verification at packing"],
}
