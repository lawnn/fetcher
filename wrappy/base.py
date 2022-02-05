import json
import sys
import os
import requests
import logging
from logging.handlers import RotatingFileHandler


class Log(object):
    def __init__(self, path):
        try:
            self.apis = self.config = json.load(open(path, 'r', encoding="utf-8"))
        except FileNotFoundError as e:
            print("[ERROR] Config file is not found.", file=sys.stderr)
            raise e
        except ValueError as e:
            print("[ERROR] Json file is invalid.", file=sys.stderr)
            raise e
        self.logger = None

        try:
            self.exchange_name = self.config["exchange_name"]
        except KeyError:
            self.exchange_name = 'Exchange'
            pass
        try:
            self.bot_name = self.config["bot_name"]
        except KeyError:
            self.bot_name = 'Bot'
        try:
            self.log_level = self.config["log_level"]
        except KeyError:
            self.log_level = 'DEBUG'
        try:
            self.log_dir = self.config["log_dir"]
        except KeyError:
            self.log_dir = 'log'

    def _initialize_logger(self):
        """
        ロガーを初期化します.
        """
        if not self.logger:
            self.logger = logging.getLogger(f"{self.exchange_name}_{self.bot_name}")
            self.logger.setLevel(self.log_level)
            stream_formatter = logging.Formatter(fmt="[%(levelname)s] %(asctime)s : %(message)s",
                                                 datefmt="%Y-%m-%d %H:%M:%S")
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(stream_formatter)
            stream_handler.setLevel(self.log_level)
            self.logger.addHandler(stream_handler)
            if self.log_dir:
                # コンフィグファイルでログディレクトリが指定されていた場合、ファイルにも出力します.
                if not os.path.exists(self.log_dir):
                    os.mkdir(self.log_dir)
                file_formatter = logging.Formatter(fmt="[%(levelname)s] %(asctime)s %(module)s: %(message)s",
                                                   datefmt="%Y-%m-%d %H:%M:%S")
                file_handler = RotatingFileHandler(
                    filename=os.path.join(self.log_dir, f"{self.exchange_name}_{self.bot_name}.log"),
                    maxBytes=1024 * 1024 * 2, backupCount=3)
                file_handler.setFormatter(file_formatter)
                file_handler.setLevel(self.log_level)
                self.logger.addHandler(file_handler)

    def log_error(self, message):
        """
        ERRORレベルのログを出力します.
        :param message: ログメッセージ.
        """
        self.logger.error(message)

    def log_warning(self, message):
        """
        WARNINGレベルのログを出力します.
        :param message: ログメッセージ.
        """
        self.logger.warning(message)

    def log_info(self, message):
        """
        INFOレベルのログを出力します.
        :param message: ログメッセージ.
        """
        self.logger.info(message)

    def log_debug(self, message):
        """
        DEBUGレベルのログを出力します.
        :param message: ログメッセージ.
        """
        self.logger.debug(message)


class Notify(Log):
    def __init__(self, path):
        super().__init__(path)

        # ラインに稼働状況を通知
        self.line_notify_token = self.config["line_notify_token"]
        self.line_notify_api = 'https://notify-api.line.me/api/notify'
        # Discordに稼働状況を通知するWebHook
        try:
            self.discordWebhook = self.config["discordWebhook"]
        except KeyError:
            # 設定されていなければNoneにしておく
            self.discordWebhook = None

    def _lineNotify(self, message, fileName=None):
        payload = {'message': message}
        headers = {'Authorization': 'Bearer ' + self.line_notify_token}
        if fileName is None:
            try:
                requests.post(self.line_notify_api, data=payload, headers=headers)
            except Exception as e:
                self.log_error(e)
                pass
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post(self.line_notify_api, data=payload, headers=headers, files=files)
            except Exception as e:
                self.log_error(e)
                pass

    # config.json内の[discordWebhook]で指定されたDiscordのWebHookへの通知
    def _discordNotify(self, message, fileName=None):
        payload = {"content": " " + message + " "}
        if fileName is None:
            try:
                requests.post(self.discordWebhook, data=payload)
            except Exception as e:
                self.log_error(e)
                pass
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post(self.discordWebhook, data=payload, files=files)
            except Exception as e:
                self.log_error(e)
                pass

    def statusNotify(self, message, fileName=None):
        # config.json内に[discordWebhook]が設定されていなければLINEへの通知
        if self.discordWebhook is None:
            self._lineNotify(message, fileName)
        else:
            # config.json内に[discordWebhook]が設定されていればDiscordへの通知
            self._discordNotify(message, fileName)


class BotBase(Notify):
    def __init__(self, path):
        super().__init__(path)
        self._initialize_logger()
