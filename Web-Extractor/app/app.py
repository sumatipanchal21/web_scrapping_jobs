from flask import Flask, render_template, request, redirect, send_file,Response, session
from extractor.indeed_extractor import ExtractIndeed
import pandas as pd
from extractor.dice_scrap import extract_dice_jobs
from flask_session import Session
from flask_sqlalchemy import SQLAlchemy
import os
from flask_ngrok import run_with_ngrok
from celery import Celery

app = Flask(__name__)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)
run_with_ngrok(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///scrapper.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
@app.route("/")
def home():
    return render_template("home.html", name="JJ")

class User(db.Model):
    id = db.Column('User_id', db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    email = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), unique=True, nullable=False)


with app.app_context():
    db.create_all()




@app.route('/')
def view():
    return "Hello, Flask is up and running!"


# @app.route("/")
# def home():
#     return render_template("home.html", name="JJ")


@app.route("/signup", methods=('GET', 'POST'))
def signup():
    message = None
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        user_exits = User.query.filter_by(email=email).first()
        if user_exits:
            message = "Email if is already Taken !!!"
            return render_template("signup.html", name="signup", message=message)

        else:
            hashed_password = generate_password_hash(
                password=password, method='sha256')
            new_user = User(username=username,
                            email=email,
                            password=hashed_password)

            db.session.add(new_user)
            db.session.commit()
            message = "New User Registerd"

            print("new user created")

    return render_template("signup.html", name="signup", message=message)


@app.route("/login", methods=('GET', 'POST'))
def login():
    error = None
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = User.query.filter_by(email=email).first()
        if user:
            if check_password_hash(user.password, password):
                print("User Logged In")
                return jsonify({'message': 'Logged in successfully!'})
                # return redirect(url_for('home'))
            else:
                error = "Wrong password"
                return render_template("login.html", name="login", error=error)
        else:
            error = "Email id not matched"
            return render_template("login.html", name="login", error=error)

    else:
        return render_template("login.html", name="login")

@app.route("/search")
def search():
    web = request.args.get("web")
    session["web"] = request.args.get("web")
    tech = request.args.get("tech")
    df = None
    name = None
    #page = request.args.get("page")
    #if page == None:
        #page = 2
    print(web, tech)
    if web == None or tech == None:
        return redirect("/")
    elif web == "indeed":
        name = "Indeed Data"
        scrap_naukri = ExtractIndeed(tech)
        scrap_naukri.scrap_details()
        scrap_naukri.generate_csv()
        df = pd.read_csv("./static/indeed_jobs_python.csv")
    elif web == "dice":
        name = "Dice Data"
        extract_dice_jobs(tech)
        df = pd.read_csv("./static/job_dice.csv")
    return render_template("search.html", tables=[df.to_html(classes='data', justify='center')], titles=df.columns.values, name=name)


@app.route("/export")
def export():
    web =  session.get("web")
    csv_dir = "./static"
    if web == "indeed":
        csv_file = 'indeed_jobs_python.csv'
        csv_path = os.path.join(csv_dir, csv_file)
        return send_file(csv_path,as_attachment=True)
    elif web == "dice":
        csv_file = 'job_dice.csv'
        csv_path = os.path.join(csv_dir, csv_file)
        return send_file(csv_path, as_attachment=True)
    else:
        return redirect("/")


if __name__ == "__main__":
    app.run()
