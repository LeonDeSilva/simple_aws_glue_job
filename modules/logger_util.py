import datetime


class Logger:
    def __init__(self, class_name):
        self.class_name = class_name

    def info(self, message):
        print(datetime.datetime.now(), ' ', self.class_name, ' [INFO] ', message)

    def error(self, message):
        print(datetime.datetime.now(), ' ', self.class_name, ' [ERROR] ', message)