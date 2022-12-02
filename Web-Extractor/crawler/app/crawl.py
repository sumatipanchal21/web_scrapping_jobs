from flask import Flask, send_file, jsonify, url_for
from flask import redirect, request, session, render_template
import os
from celery import Celery
from celery.states import state, PENDING, SUCCESS
from flask_session import Session
import pandas as pd
from celery.result import AsyncResult
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash,check_password_hash
from login_required_decorator import login_required
import csv
import mysql.connector
from flask_migrate import Migrate


app = Flask(__name__)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
celery = Celery(app.name, broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
sess = Session()
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:12345@localhost/scrap'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)


class User(db.Model):
    id = db.Column('User_id', db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    email = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), unique=True, nullable=False)


class Dicedata(db.Model):
    id = db.Column( db.Integer, primary_key=True)
    Job_Title = db.Column(db.String(500), unique=False, nullable=False)
    Company_Name = db.Column(db.String(500), unique=False, nullable=False)
    description = db.Column(db.String(1000), unique=False, nullable=False)
    Posted_Date = db.Column(db.String(100), unique=False, nullable=False)
    Job_Type = db.Column(db.String(300), unique=False, nullable=False)
    Location = db.Column(db.String(300), unique=False, nullable=False)


class Indeeddata(db.Model):
    id = db.Column( db.Integer, primary_key=True)
    Company_Name = db.Column(db.String(500), unique=False, nullable=False)
    Company_url = db.Column(db.String(500), unique=False, nullable=False)
    salary = db.Column(db.String(1000), unique=False, nullable=False)
    designation = db.Column(db.String(100), unique=False, nullable=False)
    location = db.Column(db.String(300), unique=False, nullable=False)
    qualification = db.Column(db.String(300), unique=False, nullable=False)


def save_dice_data_to_db():
    data = []
    c = mysql.connector.connect(host='localhost', user='root', database='scrap', password='12345',
                                auth_plugin='mysql_native_password')
    c_obj = c.cursor()
    with open("./static/indeed.csv", 'r', encoding="latin-1") as f:
        r = csv.reader(f)
        for row in r:
            data.append(row)

    data_csv = "insert into dicedata(Job_Title,Company_Name,description,Posted_Date,Job_Type,Location) values(%s,%s,%s,%s,%s,%s)"
    c_obj.executemany(data_csv, data)
    c.commit()
    c_obj.close()


def save_indeed_data_to_db():
    data = []
    c = mysql.connector.connect(host='localhost', user='root', database='scrap', password='12345',
                                auth_plugin='mysql_native_password')
    c_obj = c.cursor()
    with open("./static/indeed.csv", 'r', encoding="latin-1") as f:
        r = csv.reader(f)
        for row in r:
            data.append(row)

    data_csv = "insert into indeeddata(Company_Name,Company_url,salary,designation,location,qualification) values(%s,%s,%s,%s,%s,%s)"
    c_obj.executemany(data_csv, data)
    c.commit()
    c_obj.close()


with app.app_context():
    db.create_all()


@app.route("/", endpoint="1")
@login_required
def home():
    return render_template("home.html")


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
                session['user'] = User
                print(session.get('user'))
                return redirect('/')
            else:
                error = "Wrong password"
                return render_template("login.html", name="login", error=error)
        else:
            error = "Email id not matched"
            return render_template("login.html", name="login", error=error)
    else:
        return render_template("login.html", name="login")

@login_required
@app.route("/logout", endpoint='2')
def logout():
    session.clear()
    return redirect('/login')


@app.route("/search", endpoint="3", methods=['GET', 'POST'])
@login_required
def search():
    web = request.args.get("web")
    session["web"] = request.args.get("web")
    tech = request.args.get("tech")
    page = request.args.get("pages")
    df = None
    name = None
    task_id = None
    if page == None:
        page = 5
    page = int(page)
    print(web, tech, page)
    if web == None or tech == None:
        return redirect("/")
    if web == "indeed":
        c = celery.send_task("tasks.scrap_details", kwargs={"page": page})
        session["task_id"] = c
        task_id = session['task_id']
        #return jsonify(), 202, {'Location': url_for('taskstatus', task_id=task_id)}
    if web == "dice":
        print("--------- DICE celery task")
        c = celery.send_task("tasks.extract_dice_jobs", kwargs={"tech": tech, "page": page})
        session["task_id"] = c
        task_id = session['task_id']
        #return render_template("home.html", task_id=task_id), 202, {'Location': url_for('taskstatus', task_id=task_id)}
    return render_template("task.html", task_id=task_id)

@app.route("/result/<task_id>", endpoint="4")
@login_required
def show_result(task_id):
    web = session.get("web")
    status = AsyncResult(task_id, app=celery)
    df = None
    name = None
    if status.ready():
        if web == "indeed":
            try:
                df = pd.read_csv("./static/indeed.csv")
            except:
                "NO DATA"
        elif web == "dice":
            try:
                df = pd.read_csv("./static/dice.csv")
            except:
                "NO DATA"
    else:
        return render_template("pending.html", task_id=task_id, state=status.state, stage=status.result)
    return render_template("search.html", tables=[df.to_html(classes='data', justify='center')], titles=df.columns.values, name=name)

'''
@app.route('/status/<task_id>')
def taskstatus(task_id):
    print("-------------status")
    task = AsyncResult(task_id, app=celery)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)
'''

@app.route("/export")
def export():
    web = session.get("web")
    csv_dir = "./static"
    if web == "indeed":
        csv_file = 'indeed.csv'
        csv_path = os.path.join(csv_dir, csv_file)
        return send_file(csv_path, as_attachment=True)
    elif web == "dice":
        save_dice_data_to_db()
        csv_file = 'dice.csv'
        csv_path = os.path.join(csv_dir, csv_file)
        return send_file(csv_path, as_attachment=True)
    else:
        return redirect("/")


if __name__ == "__main__":
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'
    sess.init_app(app)
    app.run(debug=True)
