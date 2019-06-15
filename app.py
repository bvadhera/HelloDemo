

from flask import Flask, render_template, request, url_for,  redirect
import os, json

app = Flask(__name__)

@app.route("/")
def home():
	return "Hello"

