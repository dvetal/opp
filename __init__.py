from flask import Flask

app = Flask(__name__)
config.from_object('webconfig')

from app import views