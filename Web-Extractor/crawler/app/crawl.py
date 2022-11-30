from flask import Flask, send_file
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

app = Flask(__name__)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
celery = Celery(app.name, broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
sess = Session()
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///scrapper.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class User(db.Model):
    id = db.Column('User_id', db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    email = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), unique=True, nullable=False)


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


@app.route("/search", endpoint="3")
@login_required
def search():
    web = request.args.get("web")
    session["web"] = request.args.get("web")
    tech = request.args.get("tech")
    df = None
    name = None
    task_id = None
    page = request.args.get("pages")
    page = int(page)
    if page == None:
        page = 5
    print(web, tech, page)
    if web == None or tech == None:
        return redirect("/")
    if web == "indeed":
        c = celery.send_task("tasks.scrap_details", kwargs={"page":page})
        session["task_id"] = c
        task_id = session['task_id']

    if web == "dice":
        print("--------- DICE celery task")
        c = celery.send_task("tasks.extract_dice_jobs", kwargs={"tech":tech, "page":page})
        session["task_id"] = c
        task_id = session['task_id']
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
        return render_template("pending.html", task_id=task_id)
    return render_template("search.html", tables=[df.to_html(classes='data', justify='center')], titles=df.columns.values, name=name)


@app.route("/export")
def export():
    web = session.get("web")
    csv_dir = "./static"
    if web == "indeed":
        csv_file = 'indeed.csv'
        csv_path = os.path.join(csv_dir, csv_file)
        return send_file(csv_path, as_attachment=True)
    elif web == "dice":
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
