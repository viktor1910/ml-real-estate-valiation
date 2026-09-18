import scrapy


class PropertyItem(scrapy.Item):
    ad_id          = scrapy.Field()
    title          = scrapy.Field()
    price          = scrapy.Field()          # VND số nguyên (long)
    price_string   = scrapy.Field()          # "2.5 tỷ" — chuỗi gốc
    area           = scrapy.Field()          # "60 m²" — parse ở ETL
    ward           = scrapy.Field()          # phường
    district       = scrapy.Field()          # quận/huyện
    city           = scrapy.Field()          # thành phố
    property_type  = scrapy.Field()          # Loại BĐS
    ad_type        = scrapy.Field()          # "s"=bán, "r"=thuê
    bedrooms       = scrapy.Field()          # phòng ngủ
    bathrooms      = scrapy.Field()          # nhà vệ sinh (field wc)
    floors         = scrapy.Field()          # số tầng
    direction      = scrapy.Field()          # hướng
    interior       = scrapy.Field()          # nội thất
    legal          = scrapy.Field()          # pháp lý
    apartment_type = scrapy.Field()          # loại căn hộ
    latitude       = scrapy.Field()
    longitude      = scrapy.Field()
    url            = scrapy.Field()
    posted_at      = scrapy.Field()          # ISO datetime
    crawled_at     = scrapy.Field()          # ISO datetime
