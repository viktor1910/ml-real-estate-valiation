import json
import re
from datetime import datetime

import scrapy
from realestate.items import PropertyItem

DIRECTION_MAP = {
    1: "Đông", 2: "Tây", 3: "Nam", 4: "Bắc",
    5: "Đông Nam", 6: "Tây Nam", 7: "Đông Bắc", 8: "Tây Bắc",
}

CATEGORY_MAP = {
    1000: "Nhà đất",
    1010: "Căn hộ/Chung cư",
    1020: "Nhà ở",
    1030: "Đất",
    1040: "Văn phòng/Mặt bằng",
    1050: "Phòng trọ",
}

# Các map dưới đây là ước lượng — verify bằng DevTools lần đầu chạy.
# Nếu API trả string thay vì int, map sẽ miss và fallback về raw value (không crash).
INTERIOR_MAP = {
    1: "Nội thất đầy đủ",
    2: "Nội thất cơ bản",
    3: "Không nội thất",
}

LEGAL_MAP = {
    1: "Sổ đỏ/Sổ hồng",
    2: "Hợp đồng mua bán",
    3: "Giấy tờ hợp lệ khác",
    4: "Đang chờ sổ",
    5: "Sổ chung",
    6: "Khác",
}

APARTMENT_TYPE_MAP = {
    1: "Chung cư",
    2: "Duplex",
    3: "Penthouse",
    4: "Studio",
    5: "Officetel",
    6: "Shophouse",
}

CATEGORIES = [1000, 1010, 1020, 1030]


class ChotOtSpider(scrapy.Spider):
    name = "chotot"
    allowed_domains = ["gateway.chotot.com"]

    API_BASE = "https://gateway.chotot.com/v1/public/ad-listing"
    LIMIT = 20
    REGION_HCM = 13000            # region_v2 chotot cho Tp Hồ Chí Minh — lọc ngay API, khỏi cào toàn quốc

    custom_settings = {
        "CLOSESPIDER_ITEMCOUNT": 1000,
        "DOWNLOAD_DELAY": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "Referer": "https://nha.chotot.com/",
            "Origin": "https://nha.chotot.com",
        },
    }

    async def start(self):
        for cg in CATEGORIES:
            yield scrapy.Request(
                url=self._build_url(cg=cg, page=1),
                callback=self.parse_listing,
                cb_kwargs={"cg": cg, "page": 1},
                errback=self.handle_error,
            )

    def parse_listing(self, response, cg, page):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON parse error cg={cg} page={page}")
            return

        ads = data.get("ads", [])
        if not ads:
            return

        for ad in ads:
            item = self._parse_ad(ad)
            if item:
                yield item

        if len(ads) >= self.LIMIT and page < 50:
            yield scrapy.Request(
                url=self._build_url(cg=cg, page=page + 1),
                callback=self.parse_listing,
                cb_kwargs={"cg": cg, "page": page + 1},
                errback=self.handle_error,
            )

    def _parse_ad(self, ad):
        item = PropertyItem()

        ad_id = ad.get("ad_id") or ad.get("list_id")
        if not ad_id:
            return None

        item["ad_id"] = str(ad_id)
        item["url"] = f"https://nha.chotot.com/tin/{ad_id}"
        item["crawled_at"] = datetime.utcnow().isoformat()

        # Ngày đăng
        orig_ts = ad.get("orig_list_time") or ad.get("list_time")
        item["posted_at"] = (
            datetime.fromtimestamp(orig_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
            if orig_ts else None
        )

        item["title"] = ad.get("subject") or ad.get("title")

        # Giá
        price_raw = ad.get("price")
        item["price"] = str(price_raw) if price_raw is not None else None
        item["price_string"] = ad.get("price_string")

        # Diện tích
        size = ad.get("size") or ad.get("area_usable")
        item["area"] = f"{size} m²" if size else None

        # Địa chỉ
        item["ward"]     = ad.get("ward_name", "") or None
        item["district"] = ad.get("area_name", "") or None
        item["city"]     = ad.get("region_name", "") or None

        # Loại BĐS & loại tin
        cat = ad.get("category")
        item["property_type"] = CATEGORY_MAP.get(cat, ad.get("category_name"))
        item["ad_type"] = ad.get("type")  # "s"=bán, "r"=thuê

        # Phòng ngủ
        item["bedrooms"] = str(ad.get("rooms")) if ad.get("rooms") is not None else None

        # Nhà vệ sinh
        wc = ad.get("wc") or ad.get("bathrooms")
        item["bathrooms"] = str(wc) if wc is not None else None

        # Số tầng
        floors = ad.get("floors")
        item["floors"] = str(floors) if floors is not None else None

        # Hướng
        dir_code = ad.get("direction")
        item["direction"] = DIRECTION_MAP.get(dir_code, str(dir_code) if dir_code else None)

        # interior/legal không có ở listing endpoint — lấy từ seo_structure nếu có
        seo_items = (
            ad.get("feature_params", {})
            .get("seo_structure", {})
            .get("items", [])
        )
        seo = {s["id"]: s["value"] for s in seo_items if "id" in s}

        interior_raw = ad.get("interior")  # thường None ở listing API
        item["interior"] = seo.get("furnishing_sell") or (
            INTERIOR_MAP.get(interior_raw, str(interior_raw)) if interior_raw is not None else None
        )

        legal_raw = ad.get("legal")  # thường None ở listing API
        item["legal"] = seo.get("legal_documents") or (
            LEGAL_MAP.get(legal_raw, str(legal_raw)) if legal_raw is not None else None
        )

        self.logger.debug(
            f"[ad={ad_id}] interior={item['interior']!r} legal={item['legal']!r} "
            f"apt_type={ad.get('apartment_type')!r} ad_type={ad.get('type')!r}"
        )

        # Loại căn hộ
        apt_raw = ad.get("apartment_type")
        item["apartment_type"] = APARTMENT_TYPE_MAP.get(apt_raw, str(apt_raw) if apt_raw is not None else None)

        # Tọa độ
        item["latitude"]  = ad.get("latitude")
        item["longitude"] = ad.get("longitude")

        # Bổ sung từ body text nếu thiếu floors/bedrooms
        body_html = ad.get("body", "")
        if body_html:
            import re as _re
            from html.parser import HTMLParser

            class _Strip(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self._parts = []
                def handle_data(self, d):
                    self._parts.append(d)
                def get_text(self):
                    return " ".join(self._parts)

            p = _Strip()
            p.feed(body_html)
            body_text = p.get_text()

            if not item["floors"]:
                m = _re.search(r"(\d+)\s*tầng", body_text, _re.IGNORECASE)
                item["floors"] = m.group(1) if m else None

            if not item["bedrooms"]:
                m = _re.search(r"(\d+)\s*(phòng ngủ|PN|pn)", body_text, _re.IGNORECASE)
                item["bedrooms"] = m.group(1) if m else None

        if not item["title"] or item["price"] is None:
            return None

        return item

    def _build_url(self, cg, page):
        # HCM (region_v2) + phân trang bằng OFFSET o=(page-1)*limit — chotot bỏ qua param `page`,
        # chỉ `o` mới tiến trang (đã verify: page=1 == page=2, o=0 != o=20).
        offset = (page - 1) * self.LIMIT
        return f"{self.API_BASE}?cg={cg}&region_v2={self.REGION_HCM}&o={offset}&limit={self.LIMIT}"

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}")
