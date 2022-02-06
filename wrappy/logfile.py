import os
import json
import csv
from datetime import datetime, timezone, timedelta


class LogBase(object):
    """
    ログファイルの基底クラス.
    """

    def __init__(self, full_path, as_new=False, encoding="shift_jis", with_header=True, delimiter=",",
                 line_terminator="\n"):
        """
        コンストラクタです.
        :param full_path: ログファイルのフルパス.
        :param as_new: 既存ファイルがある場合、新規で作成するか否か.
        :param encoding: ログファイルのエンコーディング.
        :param with_header: 新規で作成する際、ヘッダをつけるか否か.
        :param delimiter: カラムの区切り文字.
        :param line_terminator: 行の終端を表す文字.
        """
        self.full_path = full_path
        self.as_new = as_new
        self.encoding = encoding
        self.with_header = with_header
        self.delimiter = delimiter
        self.line_terminator = line_terminator
        self.file = None
        self.writer = None
        self.columns = {}
        self.default_values = {}

    def open(self):
        """
        ログファイルをオープンします.
        """
        self._initialize()
        self.writer = csv.writer(self.file, lineterminator=self.line_terminator)

    def close(self):
        """
        ログファイルをクローズします.
        """
        if self.file is not None:
            self.file.close()

    def write_row(self, row: list):
        """
        リスト形式で行を書き込みます.
        :param row: 行データ.
        """
        if self.writer is None:
            raise Exception("writer is not initialized.")
        self.writer.writerow(row)

    def write_row_by_dict(self, row_dict: dict):
        """
        辞書形式で行を書き込みます. 辞書内にないカラムは空文字列とします.
        :param row_dict: 行データ.
        """
        if self.writer is None:
            raise Exception("Writer is not initialized.")
        row = []
        for item in self.get_headers():
            if item in row_dict and row_dict[item] is not None:
                row.append(row_dict[item])
            else:
                row.append("")
        self.write_row(row)

    def get_full_path(self):
        """
        ログファイルのフルパスを取得します.
        :return: ログファイルのフルパス.
        """
        return self.full_path

    def get_headers(self):
        """
        ヘッダーを取得.
        :return: ヘッダー.
        """
        return self.columns.keys()

    def get_columns(self):
        u"""
        カラムを取得.
        :return: カラム.
        """
        return self.columns

    def get_columns_num(self):
        """
        カラムの数を取得します.
        :return: カラム数.
        """
        return len(self.get_columns())

    def get_new_record(self):
        """
        新しいレコードを取得します.
        デフォルト値が設定されている場合、デフォルト値を入れます.
        :return: 新しいレコード.
        """
        record = {}
        for item in self.get_headers():
            record[item] = self.default_values[item] if item in self.default_values else None
        return record

    def _initialize(self):
        """
        ログファイルを初期化します.
        必要に応じてヘッダー行をつけます.
        """
        if self.as_new or not os.path.isfile(self.full_path):
            self.file = open(self.full_path, "w", encoding=self.encoding)
            if self.with_header:
                self.file.write(self.delimiter.join(self.get_headers()) + self.line_terminator)
            else:
                self.file.write("")
        else:
            self.file = open(self.full_path, "a", encoding=self.encoding)


class OrderHistory(LogBase):
    """
    注文履歴ログ.
    """

    def __init__(self, full_path, columns):
        super().__init__(full_path, as_new=False, encoding="shift_jis", with_header=True)
        self.columns = columns


class History:
    def __init__(self, path, columns):
        self.config = json.load(open(path, 'r', encoding="utf-8"))
        try:
            self.exchange_name = self.config["exchange_name"]
        except KeyError:
            self.exchange_name = 'Exchange'
            pass
        try:
            self.bot_name = self.config["bot_name"]
        except KeyError:
            self.bot_name = 'Bot'

        # 発注履歴を保存するファイル用のパラメータ.
        self.order_history_file_class = OrderHistory
        try:
            self.order_history_dir = self.config["log_dir"]
        except KeyError:
            self.order_history_dir = 'log'

        if not os.path.exists(self.order_history_dir):
            os.mkdir(self.order_history_dir)

        self.order_history_file_name_base = f"{self.exchange_name}_{self.bot_name}_order_history"
        self.order_history_files = {}
        self.order_history_encoding = "shift_jis"
        self.JST = timezone(timedelta(hours=9), 'JST')
        self.GMT = timezone(timedelta(hours=0), 'GMT')
        self.columns = columns

    def now_jst(self):
        """
        現在時刻をJSTで取得.
        :return: datetime.
        """
        return datetime.now(self.JST)

    def now_jst_str(self, date_format="%Y-%m-%d %H:%M:%S"):
        return self.now_jst().strftime(date_format)

    def now_gmt(self):
        """
        現在時刻をGMTで取得.
        :return: datetime
        """
        return datetime.now(self.GMT)

    def write_order_history(self, order_history):
        """
        発注履歴を出力します.
        :param order_history: ログデータ.
        """
        self.get_or_create_order_history_file().write_row_by_dict(order_history)

    def get_or_create_order_history_file(self):
        """
        現在時刻を元に発注履歴ファイルを取得します.
        ファイルが存在しない場合、新規で作成します.
        :return: 発注履歴ファイル.
        """
        today_str = self.now_jst_str("%y%m%d")
        order_history_file_name = self.order_history_file_name_base + f"_{today_str}.csv"
        full_path = self.order_history_dir + "/" + order_history_file_name
        if today_str not in self.order_history_files:
            self.order_history_files[today_str] = self.order_history_file_class(full_path, self.columns)
            self.order_history_files[today_str].open()
        return self.order_history_files[today_str]

    def close_order_history_files(self):
        """
        発注履歴ファイルをクローズします.
        """
        for order_history_file in self.order_history_files.values():
            order_history_file.close()
