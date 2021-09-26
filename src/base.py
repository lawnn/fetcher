import json
import sys
import requests


class Notify(object):
    def __init__(self, path):
        try:
            self.config = json.load(open(path, 'r', encoding="utf-8"))
        except FileNotFoundError as e:
            print("[ERROR] Config file is not found.", file=sys.stderr)
            raise e
        except ValueError as e:
            print("[ERROR] Json file is invalid.", file=sys.stderr)
            raise e

        # ラインに稼働状況を通知
        self.line_notify_token = self.config["line_notify_token"]
        self.line_notify_api = 'https://notify-api.line.me/api/notify'
        # Discordに稼働状況を通知するWebHook
        try:
            self.discordWebhook = self.config["discordWebhook"]
        except:
            # 設定されていなければNoneにしておく
            self.discordWebhook = None

    def lineNotify(self, message, fileName=None):
        payload = {'message': message}
        headers = {'Authorization': 'Bearer ' + self.line_notify_token}
        if fileName is None:
            try:
                requests.post(self.line_notify_api, data=payload, headers=headers)
            except:
                pass
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post(self.line_notify_api, data=payload, headers=headers, files=files)
            except:
                pass

    # config.json内の[discordWebhook]で指定されたDiscordのWebHookへの通知
    def discordNotify(self, message, fileName=None):
        payload = {"content": " " + message + " "}
        if fileName is None:
            try:
                requests.post(self.discordWebhook, data=payload)
            except:
                pass
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post(self.discordWebhook, data=payload, files=files)
            except:
                pass

    def statusNotify(self, message, fileName=None):
        # config.json内に[discordWebhook]が設定されていなければLINEへの通知
        if self.discordWebhook is None:
            self.lineNotify(message, fileName)
        else:
            # config.json内に[discordWebhook]が設定されていればDiscordへの通知
            self.discordNotify(message, fileName)


class BotBase(Notify):
    def __init__(self, path):
        super().__init__(path)
        self.apis = self.config
