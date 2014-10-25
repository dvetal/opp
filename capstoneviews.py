from flask import render_template, redirect
import capstoneforms
from werkzeug import secure_filename
from flask import Flask, session
import clustertext as ct
import os
from flask_bootstrap import Bootstrap

UPLOAD_FOLDER = 'data/resume/'
ALLOWED_EXTENSIONS = set(['txt'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config.from_object('webconfig')
bootstrap = Bootstrap(app)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET','POST'])
def fillForm():
    person = capstoneforms.Person()
    if person.validate_on_submit() and allowed_file(person.resume.data.filename):
        resume = secure_filename(person.resume.data.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'] + resume)
        person.resume.data.save(file_path)
        some_history = [{'company': person.data['workplace1'],
                                 'rating': int(person.data['workrating1'])},
                                {'company': person.data['workplace2'],
                                 'rating': int(person.data['workrating2'])},
                                {'company': person.data['workplace3'],
                                 'rating': int(person.data['workrating3'])}
                                ]
        some_person = {
            'title': person.data['title'],
            'age': int(person.data['age']),
            'industry': person.data['industry'],
            'size': int(person.data['size']),
            'resume': (file_path),
            'type': int(person.data['type'])
        }
        result = ct.run_master_ranker(person=some_person, history=some_history)
        result = list(result['cname'])
        session['results'] = result
        return redirect('/result')
    return render_template('person.html', title='Outmaneuver', form=person)

@app.route('/result')
def results():
    results = session['results']
    return render_template('result.html', results=results)


app.debug = True
app.run()
