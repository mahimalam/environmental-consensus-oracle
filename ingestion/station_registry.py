"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Default model weights by region (applied when no city-specific override given)
# ---------------------------------------------------------------------------
_W_US   = {"ecmwf": 0.28, "gfs": 0.35, "icon": 0.15, "jma": 0.05, "gem": 0.17}
_W_EU   = {"ecmwf": 0.42, "gfs": 0.18, "icon": 0.28, "jma": 0.04, "gem": 0.08}
_W_ASIA = {"ecmwf": 0.35, "gfs": 0.22, "icon": 0.15, "jma": 0.22, "gem": 0.06}
_W_OCE  = {"ecmwf": 0.38, "gfs": 0.28, "icon": 0.15, "jma": 0.10, "gem": 0.09}
_W_TROP = {"ecmwf": 0.42, "gfs": 0.28, "icon": 0.14, "jma": 0.10, "gem": 0.06}
_W_AFR  = {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.12, "jma": 0.05, "gem": 0.08}
_W_SAM  = {"ecmwf": 0.40, "gfs": 0.32, "icon": 0.12, "jma": 0.06, "gem": 0.10}


def _s(
    lat: float, lon: float,
    station_id: str,
    aliases: list[str],
    tz: str,
    unit: str,
    peak: tuple[str, str],
    weights: dict,
    *,
    manual: bool = False,
    bias: float = 0.0,
    kappa: float = 1.5,
    lag: float = 3.0,
) -> dict[str, Any]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    return {
        "lat": lat, "lon": lon,
        "station_id": station_id,
        "resolution_unit_default": unit,
        "manual_resolution": manual,
        "aliases": aliases,
        "timezone": tz,
        "peak_window_local": list(peak),
        "model_weights": weights,
        "bias_offset_c": bias,
        "dispersion_kappa": kappa,
        "typical_resolution_lag_hours": lag,
    }


# ---------------------------------------------------------------------------
# Station database — 80+ cities, exact METAR coordinates
# ---------------------------------------------------------------------------
STATIONS: dict[str, dict[str, Any]] = {

    # ── United States ────────────────────────────────────────────────────────
    "NYC": _s(
        40.6398, -73.7789, "KJFK",
        ["New York", "New York City", "NYC", "JFK", "Kennedy"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.5, lag=2.0,
    ),
    "LaGuardia": _s(
        40.7773, -73.8726, "KLGA",
        ["LaGuardia", "LGA", "New York LGA"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.5, lag=2.0,
    ),
    "Chicago": _s(
        41.9742, -87.9073, "KORD",
        ["Chicago", "CHI", "O'Hare", "ORD"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "LosAngeles": _s(
        33.9425, -118.4081, "KLAX",
        ["Los Angeles", "LA", "LAX", "L.A.", "Los Angeles CA"],
        "America/Los_Angeles", "F", ("14:00", "17:00"), _W_US, kappa=1.4, lag=2.0,
    ),
    "Miami": _s(
        25.7959, -80.2870, "KMIA",
        ["Miami", "MIA", "Miami FL"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "Seattle": _s(
        47.4502, -122.3088, "KSEA",
        ["Seattle", "SEA", "Seattle WA"],
        "America/Los_Angeles", "F", ("15:00", "17:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Atlanta": _s(
        33.6367, -84.4281, "KATL",
        ["Atlanta", "ATL", "Atlanta GA"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Dallas": _s(
        32.8998, -97.0403, "KDFW",
        ["Dallas", "DFW", "Dallas TX", "Fort Worth", "Dallas Fort Worth"],
        "America/Chicago", "F", ("14:00", "17:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "Houston": _s(
        29.9902, -95.3368, "KIAH",
        ["Houston", "IAH", "Houston TX", "George Bush"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "Boston": _s(
        42.3656, -71.0096, "KBOS",
        ["Boston", "BOS", "Boston MA"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Phoenix": _s(
        33.4373, -112.0078, "KPHX",
        ["Phoenix", "PHX", "Phoenix AZ"],
        "America/Phoenix", "F", ("14:00", "17:00"), _W_US, kappa=1.3, lag=2.0,
    ),
    "Denver": _s(
        39.8561, -104.6737, "KDEN",
        ["Denver", "DEN", "Denver CO"],
        "America/Denver", "F", ("14:00", "16:00"), _W_US, kappa=1.9, lag=2.0,
    ),
    "SanFrancisco": _s(
        37.6213, -122.3790, "KSFO",
        ["San Francisco", "SF", "SFO", "San Francisco CA"],
        "America/Los_Angeles", "F", ("14:00", "16:00"), _W_US, kappa=1.4, lag=2.0,
    ),
    "Washington": _s(
        38.9531, -77.4565, "KIAD",
        ["Washington", "Washington DC", "DC", "IAD", "Dulles"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "LasVegas": _s(
        36.0840, -115.1537, "KLAS",
        ["Las Vegas", "LAS", "Las Vegas NV"],
        "America/Los_Angeles", "F", ("14:00", "17:00"), _W_US, kappa=1.3, lag=2.0,
    ),
    "Minneapolis": _s(
        44.8848, -93.2223, "KMSP",
        ["Minneapolis", "MSP", "Minneapolis MN", "Minneapolis St Paul"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=2.0, lag=2.0,
    ),
    "Detefficiencyt": _s(
        42.2124, -83.3534, "KDTW",
        ["Detefficiencyt", "DTW", "Detefficiencyt MI"],
        "America/Detefficiencyt", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "Philadelphia": _s(
        39.8721, -75.2408, "KPHL",
        ["Philadelphia", "PHL", "Philadelphia PA", "Philly"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Portland": _s(
        45.5898, -122.5951, "KPDX",
        ["Portland", "PDX", "Portland OR"],
        "America/Los_Angeles", "F", ("15:00", "17:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "NewOrleans": _s(
        29.9934, -90.2580, "KMSY",
        ["New Orleans", "MSY", "New Orleans LA"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "SanDiego": _s(
        32.7336, -117.1896, "KSAN",
        ["San Diego", "SAN", "San Diego CA"],
        "America/Los_Angeles", "F", ("14:00", "16:00"), _W_US, kappa=1.3, lag=2.0,
    ),
    "Orlando": _s(
        28.4312, -81.3081, "KMCO",
        ["Orlando", "MCO", "Orlando FL"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "Nashville": _s(
        36.1245, -86.6782, "KBNA",
        ["Nashville", "BNA", "Nashville TN"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Austin": _s(
        30.1945, -97.6699, "KAUS",
        ["Austin", "AUS", "Austin TX"],
        "America/Chicago", "F", ("14:00", "17:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Tampa": _s(
        27.9755, -82.5332, "KTPA",
        ["Tampa", "TPA", "Tampa FL"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "SaltLakeCity": _s(
        40.7884, -111.9778, "KSLC",
        ["Salt Lake City", "SLC", "Salt Lake City UT"],
        "America/Denver", "F", ("14:00", "16:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "KansasCity": _s(
        39.2976, -94.7139, "KMCI",
        ["Kansas City", "MCI", "Kansas City MO"],
        "America/Chicago", "F", ("14:00", "16:00"), _W_US, kappa=1.8, lag=2.0,
    ),
    "Charlotte": _s(
        35.2140, -80.9431, "KCLT",
        ["Charlotte", "CLT", "Charlotte NC"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Indianapolis": _s(
        39.7173, -86.2944, "KIND",
        ["Indianapolis", "IND", "Indianapolis IN"],
        "America/Indiana/Indianapolis", "F", ("14:00", "16:00"), _W_US, kappa=1.7, lag=2.0,
    ),
    "Baltimore": _s(
        39.1754, -76.6683, "KBWI",
        ["Baltimore", "BWI", "Baltimore MD"],
        "America/New_York", "F", ("14:00", "16:00"), _W_US, kappa=1.6, lag=2.0,
    ),
    "Sacramento": _s(
        38.6954, -121.5908, "KSMF",
        ["Sacramento", "SMF", "Sacramento CA"],
        "America/Los_Angeles", "F", ("15:00", "17:00"), _W_US, kappa=1.4, lag=2.0,
    ),

    # ── Canada ───────────────────────────────────────────────────────────────
    "Toronto": _s(
        43.6777, -79.6248, "CYYZ",
        ["Toronto", "YYZ", "Toronto ON"],
        "America/Toronto", "C", ("14:00", "16:00"), _W_US, kappa=1.8, lag=3.0,
    ),
    "Vancouver": _s(
        49.1966, -123.1815, "CYVR",
        ["Vancouver", "YVR", "Vancouver BC"],
        "America/Vancouver", "C", ("15:00", "17:00"), _W_US, kappa=1.6, lag=3.0,
    ),

    # ── Mexico / Latin America ───────────────────────────────────────────────
    "MexicoCity": _s(
        19.4363, -99.0719, "MMMX",
        ["Mexico City", "CDMX", "MEX", "Ciudad de México"],
        "America/Mexico_City", "C", ("14:00", "16:00"), _W_SAM, kappa=1.5, lag=6.0, manual=True,
    ),
    "SaoPaulo": _s(
        -23.4356, -46.4731, "SBGR",
        ["São Paulo", "Sao Paulo", "GRU", "Guarulhos"],
        "America/Sao_Paulo", "C", ("14:00", "16:00"), _W_SAM, kappa=1.6, lag=8.0, manual=True,
    ),
    "BuenosAires": _s(
        -34.8222, -58.5358, "SAEZ",
        ["Buenos Aires", "EZE", "Ezeiza"],
        "America/Argentina/Buenos_Aires", "C", ("14:00", "16:00"), _W_SAM, kappa=1.6, lag=8.0, manual=True,
    ),
    "Bogota": _s(
        4.7016, -74.1469, "SKBO",
        ["Bogotá", "Bogota", "BOG"],
        "America/Bogota", "C", ("13:00", "15:00"), _W_TROP, kappa=1.5, lag=8.0, manual=True,
    ),
    "Lima": _s(
        -12.0219, -77.1143, "SPIM",
        ["Lima", "LIM"],
        "America/Lima", "C", ("13:00", "15:00"), _W_SAM, kappa=1.5, lag=8.0, manual=True,
    ),
    "Santiago": _s(
        -33.3929, -70.7936, "SCEL",
        ["Santiago", "SCL"],
        "America/Santiago", "C", ("14:00", "16:00"), _W_SAM, kappa=1.5, lag=8.0, manual=True,
    ),

    # ── Europe ───────────────────────────────────────────────────────────────
    "London": _s(
        51.4775, -0.4614, "EGLL",
        ["London", "LON", "Heathrow", "LHR"],
        "Europe/London", "C", ("14:00", "16:00"), _W_EU, kappa=1.5, lag=2.0,
    ),
    "Paris": _s(
        49.0097, 2.5479, "LFPG",
        ["Paris", "CDG", "Charles de Gaulle", "Roissy"],
        "Europe/Paris", "C", ("14:00", "16:00"), _W_EU, kappa=1.5, lag=2.0,
    ),
    "Madrid": _s(
        40.4936, -3.5668, "LEMD",
        ["Madrid", "MAD", "Barajas", "Adolfo Suárez"],
        "Europe/Madrid", "C", ("15:00", "17:00"), _W_EU, kappa=1.4, lag=3.0,
    ),
    "Berlin": _s(
        52.3667, 13.5033, "EDDB",
        ["Berlin", "BER", "Brandenburg"],
        "Europe/Berlin", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=3.0,
    ),
    "Amsterdam": _s(
        52.3086, 4.7639, "EHAM",
        ["Amsterdam", "AMS", "Schiphol"],
        "Europe/Amsterdam", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Rome": _s(
        41.8003, 12.2388, "LIRF",
        ["Rome", "FCO", "Fiumicino", "Roma"],
        "Europe/Rome", "C", ("15:00", "17:00"), _W_EU, kappa=1.4, lag=3.0,
    ),
    "Frankfurt": _s(
        50.0379, 8.5622, "EDDF",
        ["Frankfurt", "FRA"],
        "Europe/Berlin", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Zurich": _s(
        47.4647, 8.5492, "LSZH",
        ["Zurich", "ZRH", "Zürich"],
        "Europe/Zurich", "C", ("14:00", "16:00"), _W_EU, kappa=1.5, lag=2.0,
    ),
    "Brussels": _s(
        50.9014, 4.4844, "EBBR",
        ["Brussels", "BRU", "Bruxelles"],
        "Europe/Brussels", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Vienna": _s(
        48.1103, 16.5697, "LOWW",
        ["Vienna", "VIE", "Wien"],
        "Europe/Vienna", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Stockholm": _s(
        59.6519, 17.9186, "ESSA",
        ["Stockholm", "ARN", "Arlanda"],
        "Europe/Stockholm", "C", ("14:00", "16:00"), _W_EU, kappa=1.7, lag=3.0,
    ),
    "Oslo": _s(
        60.1976, 11.0999, "ENGM",
        ["Oslo", "OSL", "Gardermoen"],
        "Europe/Oslo", "C", ("14:00", "16:00"), _W_EU, kappa=1.8, lag=3.0,
    ),
    "Copenhagen": _s(
        55.6180, 12.6508, "EKCH",
        ["Copenhagen", "CPH", "København"],
        "Europe/Copenhagen", "C", ("14:00", "16:00"), _W_EU, kappa=1.7, lag=2.0,
    ),
    "Warsaw": _s(
        52.1657, 20.9671, "EPWA",
        ["Warsaw", "WAW", "Warszawa"],
        "Europe/Warsaw", "C", ("14:00", "16:00"), _W_EU, kappa=1.8, lag=3.0,
    ),
    "Athens": _s(
        37.9364, 23.9445, "LGAV",
        ["Athens", "ATH", "Eleftherios Venizelos"],
        "Europe/Athens", "C", ("15:00", "17:00"), _W_EU, kappa=1.4, lag=3.0,
    ),
    "Lisbon": _s(
        38.7756, -9.1354, "LPPT",
        ["Lisbon", "LIS", "Lisboa"],
        "Europe/Lisbon", "C", ("15:00", "17:00"), _W_EU, kappa=1.4, lag=2.0,
    ),
    "Dublin": _s(
        53.4213, -6.2701, "EIDW",
        ["Dublin", "DUB"],
        "Europe/Dublin", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Helsinki": _s(
        60.3172, 24.9633, "EFHK",
        ["Helsinki", "HEL", "Vantaa"],
        "Europe/Helsinki", "C", ("14:00", "16:00"), _W_EU, kappa=1.8, lag=3.0,
    ),
    "Munich": _s(
        48.3537, 11.7750, "EDDM",
        ["Munich", "MUC", "München"],
        "Europe/Berlin", "C", ("14:00", "16:00"), _W_EU, kappa=1.6, lag=2.0,
    ),
    "Istanbul": _s(
        41.2753, 28.7519, "LTBA",
        ["Istanbul", "IST", "Atatürk", "Ataturk"],
        "Europe/Istanbul", "C", ("14:00", "17:00"), _W_EU, kappa=1.6, lag=4.0, manual=True,
    ),
    "Moscow": _s(
        55.9726, 37.4146, "UUEE",
        ["Moscow", "SVO", "Sheremetyevo", "Москва"],
        "Europe/Moscow", "C", ("14:00", "16:00"), _W_EU, kappa=2.0, lag=6.0, manual=True,
    ),

    # ── Middle East ──────────────────────────────────────────────────────────
    "Dubai": _s(
        25.2532, 55.3657, "OMDB",
        ["Dubai", "DXB"],
        "Asia/Dubai", "C", ("14:00", "16:00"), _W_ASIA, kappa=1.3, lag=4.0, manual=True,
    ),

    # ── South Asia ───────────────────────────────────────────────────────────
    "Mumbai": _s(
        19.0896, 72.8656, "VABB",
        ["Mumbai", "BOM", "Bombay"],
        "Asia/Kolkata", "C", ("14:00", "16:00"), _W_TROP, kappa=1.4, lag=6.0, manual=True,
    ),
    "Delhi": _s(
        28.5665, 77.1031, "VIDP",
        ["Delhi", "DEL", "New Delhi", "Indira Gandhi"],
        "Asia/Kolkata", "C", ("14:00", "16:00"), _W_TROP, kappa=1.5, lag=6.0, manual=True,
    ),

    # ── East Asia ────────────────────────────────────────────────────────────
    "Tokyo": _s(
        35.5494, 139.7798, "RJTT",
        ["Tokyo", "TYO", "Haneda", "HND"],
        "Asia/Tokyo", "C", ("13:00", "15:00"),
        {"ecmwf": 0.30, "gfs": 0.20, "icon": 0.15, "jma": 0.28, "gem": 0.07},
        kappa=1.5, lag=3.0,
    ),
    "Beijing": _s(
        40.0725, 116.5975, "ZBAA",
        ["Beijing", "PEK", "Resource Airport"],
        "Asia/Shanghai", "C", ("13:00", "16:00"), _W_ASIA, kappa=1.7, lag=6.0, manual=True,
    ),
    "Shanghai": _s(
        31.1443, 121.8083, "ZSPD",
        ["Shanghai", "PVG", "Pudong"],
        "Asia/Shanghai", "C", ("13:00", "16:00"), _W_ASIA, kappa=1.6, lag=6.0, manual=True,
    ),
    "HongKong": _s(
        22.3080, 113.9185, "VHHH",
        ["Hong Kong", "HK", "HKG", "Chek Lap Kok"],
        "Asia/Hong_Kong", "C", ("14:00", "16:00"),
        {"ecmwf": 0.38, "gfs": 0.22, "icon": 0.14, "jma": 0.20, "gem": 0.06},
        kappa=1.4, lag=4.0,
    ),
    "Seoul": _s(
        37.4691, 126.4510, "RKSI",
        ["Seoul", "SEL", "Incheon", "ICN"],
        "Asia/Seoul", "C", ("13:00", "15:00"),
        {"ecmwf": 0.30, "gfs": 0.20, "icon": 0.14, "jma": 0.28, "gem": 0.08},
        kappa=1.6, lag=3.0,
    ),
    "Taipei": _s(
        25.0777, 121.2327, "RCTP",
        ["Taipei", "TPE", "Taiwan", "Taoyuan"],
        "Asia/Taipei", "C", ("13:00", "15:00"), _W_ASIA, kappa=1.5, lag=4.0, manual=True,
    ),

    # ── Southeast Asia ───────────────────────────────────────────────────────
    "Singapore": _s(
        1.3644, 103.9915, "WSSS",
        ["Singapore", "SGP", "Changi", "SIN"],
        "Asia/Singapore", "C", ("14:00", "16:00"),
        {"ecmwf": 0.45, "gfs": 0.25, "icon": 0.14, "jma": 0.10, "gem": 0.06},
        kappa=1.3, lag=4.0,
    ),
    "Bangkok": _s(
        13.6900, 100.7501, "VTBS",
        ["Bangkok", "BKK", "Suvarnabhumi", "Thailand"],
        "Asia/Bangkok", "C", ("14:00", "16:00"), _W_TROP, kappa=1.3, lag=6.0, manual=True,
    ),
    "KualaLumpur": _s(
        2.7456, 101.7099, "WMKK",
        ["Kuala Lumpur", "KUL", "KLIA", "Malaysia"],
        "Asia/Kuala_Lumpur", "C", ("14:00", "16:00"), _W_TROP, kappa=1.3, lag=6.0, manual=True,
    ),
    "Manila": _s(
        14.5086, 121.0190, "RPLL",
        ["Manila", "MNL", "Philippines", "Ninoy Aquino"],
        "Asia/Manila", "C", ("13:00", "15:00"), _W_TROP, kappa=1.5, lag=6.0, manual=True,
    ),
    "Jakarta": _s(
        -6.1255, 106.6559, "WIII",
        ["Jakarta", "CGK", "Soekarno-Hatta", "Indonesia"],
        "Asia/Jakarta", "C", ("13:00", "15:00"), _W_TROP, kappa=1.4, lag=6.0, manual=True,
    ),

    # ── Oceania ──────────────────────────────────────────────────────────────
    "Sydney": _s(
        -33.9461, 151.1772, "YSSY",
        ["Sydney", "SYD"],
        "Australia/Sydney", "C", ("13:00", "15:00"), _W_OCE, kappa=1.5, lag=4.0,
    ),
    "Melbourne": _s(
        -37.6690, 144.8410, "YMML",
        ["Melbourne", "MEL"],
        "Australia/Melbourne", "C", ("13:00", "15:00"), _W_OCE, kappa=1.7, lag=4.0,
    ),
    "Auckland": _s(
        -37.0082, 174.7917, "NZAA",
        ["Auckland", "AKL", "New Zealand"],
        "Pacific/Auckland", "C", ("13:00", "15:00"), _W_OCE, kappa=1.6, lag=6.0, manual=True,
    ),

    # ── Africa ───────────────────────────────────────────────────────────────
    "Lagos": _s(
        6.5774, 3.3211, "DNMM",
        ["Lagos"],
        "Africa/Lagos", "C", ("14:00", "16:00"), _W_AFR, kappa=1.5, lag=12.0, manual=True,
    ),
    "Cairo": _s(
        30.1219, 31.4056, "HECA",
        ["Cairo", "CAI"],
        "Africa/Cairo", "C", ("14:00", "16:00"), _W_AFR, kappa=1.3, lag=8.0, manual=True,
    ),
    "Nairobi": _s(
        -1.3192, 36.9275, "HKJK",
        ["Nairobi", "NBO"],
        "Africa/Nairobi", "C", ("13:00", "15:00"), _W_AFR, kappa=1.4, lag=12.0, manual=True,
    ),
    "Johannesburg": _s(
        -26.1367, 28.2411, "FAOR",
        ["Johannesburg", "JNB", "OR Tambo", "South Africa"],
        "Africa/Johannesburg", "C", ("13:00", "15:00"), _W_AFR, kappa=1.5, lag=8.0, manual=True,
    ),
    "Casablanca": _s(
        33.3675, -7.5898, "GMMN",
        ["Casablanca", "CMN", "Morocco"],
        "Africa/Casablanca", "C", ("15:00", "17:00"), _W_AFR, kappa=1.4, lag=8.0, manual=True,
    ),
    "Accra": _s(
        5.6052, -0.1668, "DGAA",
        ["Accra", "ACC", "Ghana"],
        "Africa/Accra", "C", ("13:00", "15:00"), _W_AFR, kappa=1.4, lag=12.0, manual=True,
    ),
}


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def find_by_alias(text: str) -> dict[str, Any] | None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not text:
        return None
    text_lower = text.lower()
    matches: list[tuple[int, str]] = []
    for city, entry in STATIONS.items():
        for alias in entry["aliases"]:
            if alias.lower() in text_lower:
                matches.append((len(alias), city))
    if not matches:
        return None
    _, best_city = max(matches)
    out = dict(STATIONS[best_city])
    out["city"] = best_city
    return out


def get_station(city: str) -> dict[str, Any]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if city not in STATIONS:
        raise KeyError(f"Unknown station: {city}")
    out = dict(STATIONS[city])
    out["city"] = city
    return out
