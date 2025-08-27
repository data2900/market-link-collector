import os
import sqlite3
from datetime import datetime
import scrapy

# ── DB 設定（環境変数で上書き可） ─────────────────────────────
DB_PATH = os.getenv("MARKET_DB_PATH", os.path.abspath("./market_data.db"))

class MarketLinkSpider(scrapy.Spider):
    """
    サンプルの市場系一覧ページをクロールし、
    企業コード・企業名・関連リンク（ダミー）を SQLite に保存する。
    """
    name = "market_links"
    allowed_domains = ["example.com"]  # 固有の媒体名は伏せる
    start_urls = ["https://example.com/markets/companies/"]  # ダミーURL

    def __init__(self, target_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # -a target_date=YYYYMMDD で受け取る（ポートフォリオ用）
        if not target_date:
            raise ValueError("実行時に -a target_date=YYYYMMDD の形式で日付を指定してください")
        try:
            datetime.strptime(target_date, "%Y%m%d")
        except ValueError:
            raise ValueError("日付の形式が正しくありません。YYYYMMDD形式で指定してください")
        self.target_date = target_date

        # SQLite 初期化
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        self._init_db()

        # 収集数カウンタ
        self.total_seen = 0
        self.total_inserted = 0

    # ── テーブル作成 ─────────────────────────────────────────
    def _init_db(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS consensus_url (
                target_date TEXT,
                code        TEXT,
                name        TEXT,
                link_a      TEXT,
                link_b      TEXT,
                link_c      TEXT,
                PRIMARY KEY (target_date, code)
            )
        """)
        self.conn.commit()

    # ── 終了時 ──────────────────────────────────────────────
    def closed(self, reason):
        msg = f"📦 取得件数: {self.total_seen} / 新規登録: {self.total_inserted}（date={self.target_date}）"
        self.logger.info(msg)
        print(msg)
        self.conn.close()

    # ── 解析ロジック（ダミーの一般的XPath） ────────────────────────
    def parse(self, response):
        # 一般的なテーブル行構造を想定
        rows = response.xpath('//table//tr')
        for row in rows:
            # 例：1列目に企業コード、2列目に企業名があると仮定
            code = row.xpath('./td[1]//text()').get()
            name = row.xpath('./td[2]//text()').get()

            # コードと名称が取得できた行のみ処理
            if code and name:
                self.total_seen += 1

                # ダミーの関連リンク（実在サービス名は記載しない）
                link_a = f"https://example.com/company/{code}"
                link_b = f"https://example.org/stock/{code}"
                link_c = f"https://example.net/detail/{code}"

                # INSERT OR IGNORE で重複回避
                self.cursor.execute("""
                    INSERT OR IGNORE INTO consensus_url (
                        target_date, code, name, link_a, link_b, link_c
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (self.target_date, code, name, link_a, link_b, link_c))

                if self.cursor.rowcount == 1:
                    self.total_inserted += 1

        self.conn.commit()

        # 「Next」リンクがあれば再帰追跡（一般的なUI文言を想定）
        next_page = response.xpath('//a[normalize-space(text())="Next"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
